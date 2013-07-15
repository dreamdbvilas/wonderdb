/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package org.wonderdb.txnlogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.cache.CacheWriter;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.seralizers.block.TransactionLogSerializedBlock;
import org.wonderdb.seralizers.block.TransactionLogSerializedBlockImpl;
import org.wonderdb.server.WonderDBPropertyManager;

public class LogManager {
	AtomicInteger at = new AtomicInteger();
	Queue<Integer> committingTxns = new ConcurrentLinkedQueue<Integer>();
	Map<BlockPtr, SerializedBlock> blockPtrBlockMap = new HashMap<BlockPtr, SerializedBlock>();
	
	Map<Integer, List<BlockPtr>> blockPtrsInTxn = new HashMap<Integer, List<BlockPtr>>();
	List<BlockPtr> blockPtrList = new ArrayList<BlockPtr>();
	Map<Integer, List<Integer>> depTxnMap = new HashMap<Integer, List<Integer>>();
	Queue<Integer> inflightTxns = new ConcurrentLinkedQueue<Integer>();
	
	List<RandomAccessFile> logFiles = new ArrayList<RandomAccessFile>();
	int currentFilePosn = 0;
	
	CacheWriter writer = null;
	String filePath = "";
	long currentMaxTxnTime = -1;
	boolean loggingEnabled = false;
	
	private static LogManager instance = new LogManager();

	
	private LogManager() {
		if (!WonderDBPropertyManager.getInstance().isLookAheadLoggingEnabled()) {
			return;
		}
		loggingEnabled = true;
		filePath = WonderDBPropertyManager.getInstance().getLogFilePath();
		while (true) {
			File file = null;
			file = new File("./"+filePath+"/redolog"+logFiles.size());
			if (file.exists() || logFiles.size() == 0) {
				try {
					file.createNewFile();
					RandomAccessFile raf = new RandomAccessFile(file, "rw");
					logFiles.add(raf);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				break;
			}
		}		
	}
	
	public static LogManager getInstance() {
		return instance;
	}
	
	public TransactionId startTxn() {
		if (!loggingEnabled) {
			return null;
		}
		TransactionId txnId = new TransactionId(at.getAndAdd(1));
		inflightTxns.add(txnId.getId());
		depTxnMap.put(txnId.getId(), new ArrayList<Integer>());
		List<BlockPtr> ptrList = new ArrayList<BlockPtr>();
		blockPtrsInTxn.put(txnId.getId(), ptrList);
		return txnId;
	}
	
	public synchronized void logBlock(TransactionId txnId, SerializedBlock block) {
		if (!loggingEnabled) {
			return;
		}
		inflightTxns.remove(txnId.getId());
		inflightTxns.add(txnId.getId());
		long accessTime = System.currentTimeMillis();
		block.setLastAccessTime(accessTime);
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(block.getFullBuffer());
		SerializedBlock sb = new SerializedBlockImpl(block.getPtr(), buffer);
		blockPtrBlockMap.put(block.getPtr(), sb);
		sb.setLastAccessTime(accessTime);
		List<BlockPtr> ptrList = blockPtrsInTxn.get(txnId.getId());
		ptrList.add(block.getPtr());
		
		boolean val = blockPtrList.remove(block.getPtr());
		blockPtrList.add(block.getPtr());
		
		if (!val) {
			return;
		}
		
		Iterator<Integer> txnIdIter = blockPtrsInTxn.keySet().iterator();
		List<Integer> thisTxnDepList= depTxnMap.get(txnId.getId());

		while (txnIdIter.hasNext()) {
			int id = txnIdIter.next();
			if (id == txnId.getId()) {
				continue;
			}
			List<BlockPtr> iterBlockPtrList = blockPtrsInTxn.get(id);
			if (iterBlockPtrList.contains(block.getPtr())) {
				if (!thisTxnDepList.contains(id)) {
					thisTxnDepList.add(id);
				}
				
				List<Integer> list = depTxnMap.get(id);
				if (!list.contains(txnId.getId())) {
					list.add(txnId.getId());
				}
			}
		}
	}
	
	public void commitTxn(TransactionId txnId) {
		if (!loggingEnabled) {
			return;
		}
		if (txnId == null) {
			return;
		}
		synchronized (committingTxns) {
			committingTxns.add(txnId.getId());
			logTransaction(txnId.getId());			
		}
	}
	
	private void logTransaction(int txnId) {
		while (true) {
			if (!committingTxns.contains(txnId)) {
				return;
			}
			if (committingTxns.peek() != txnId) {
				try {
					committingTxns.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {				
				committingTxns.poll();
				List<Integer> depTxnIds = depTxnMap.get(txnId);
				if (committingTxns.containsAll(depTxnIds)) {
					break;
				} 
				committingTxns.add(txnId);
				try {
					committingTxns.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		List<BlockPtr> bpList = new ArrayList<BlockPtr>();
		bpList.addAll(blockPtrsInTxn.get(txnId));
		
		List<Integer> depTxnIds = depTxnMap.get(txnId);
		for (int i = 0; i < depTxnIds.size(); i++) {
			bpList.addAll(blockPtrsInTxn.get(depTxnIds.get(i)));
		}
		
		Set<BlockPtr> bpSet = new HashSet<BlockPtr>(bpList);
		
		List<SerializedBlock> sblist = new ArrayList<SerializedBlock>();
		Iterator<BlockPtr> iter = bpSet.iterator();
		
		while (iter.hasNext()) {
			SerializedBlock sb = blockPtrBlockMap.get(iter.next()); 
			sblist.add(sb);
		}
		blockPtrList.removeAll(bpList);
		
		write(sblist);
		committingTxns.remove(txnId);
		committingTxns.removeAll(depTxnIds);
		depTxnMap.remove(txnId);
		blockPtrsInTxn.remove(txnId);
		committingTxns.notifyAll();
	}	
	
	private void write(List<SerializedBlock> list) {
		if (list == null || list.size() == 0) {
			return;
		}
		TransactionLogSerializedBlock tsb = new TransactionLogSerializedBlockImpl(list.get(0).getFullBuffer());
		tsb.setTrnsactionBlockCount(list.size());
		ChannelBuffer[] buffers = new ChannelBuffer[list.size()];
		for (int i = 0; i < list.size(); i++) {
			SerializedBlock sb = list.get(i);
			buffers[i] = sb.getFullBuffer();
			currentMaxTxnTime = Math.max(currentMaxTxnTime, sb.getLastAccessTime());
		}
		
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buffers);
		
		FileChannel channel = getFileChannel(buffer.capacity());
		ByteBuffer buf = buffer.toByteBuffer();
		try {
			channel.write(buf);
		} catch (IOException e) {
		}
	}
	
	private FileChannel getFileChannel(int requiredSize) {
		FileChannel channel = logFiles.get(currentFilePosn).getChannel();
		long currentSize = -1;
		try {
			currentSize = channel.position();
		} catch (IOException e) {
		}
		
		if (currentSize + requiredSize < 100000000) {
			return channel;
		} 

		return findOrCreateNewLogFile();
	}
	
	private void resetCurrentChannel(long time) {
		FileChannel channel = logFiles.get(currentFilePosn).getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE/8);
		buffer.putLong(System.currentTimeMillis());
		buffer.flip();
		try {
			channel.position(0);
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private FileChannel findOrCreateNewLogFile() {
		
		long currentSyncTime = writer.getSyncTime();
		
		resetCurrentChannel(currentMaxTxnTime);
		
		for (int i = 0; i < logFiles.size(); i++) {
			if (i == currentFilePosn) {
				continue;
			}
			FileChannel channel = logFiles.get(i).getChannel();
			long size = 0;
			try {
				size = channel.size();
			} catch (IOException e1) {
				e1.printStackTrace();
			} 
			if (size == 0) {
				continue;
			}
			try {
				channel.position(0);
			} catch (IOException e) {
			}
			ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE/8); 
			try {
				channel.read(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
			buffer.flip();
			long l = buffer.getLong();
			if (currentSyncTime >= l) {
				currentFilePosn = i;
				resetCurrentChannel(Long.MAX_VALUE);
				return channel;
			}
		}
		
		// create new one
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filePath+"/redolog"+logFiles.size(), "rw");
			logFiles.add(raf);
			currentFilePosn = logFiles.size()-1;
			resetCurrentChannel(Long.MAX_VALUE);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return logFiles.get(currentFilePosn).getChannel();
	}
	
	public void shutdown() {
		for (int i = 0; i < logFiles.size(); i++) {
			try {
				logFiles.get(i).setLength(0);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}
	
	public void resetLogs(long writerSyncedTime) {
		synchronized (committingTxns) {
			for (int i = 0; i < logFiles.size(); i++) {
				if (i == currentFilePosn) {
					continue;
				}
				RandomAccessFile raf = logFiles.get(i);
				FileChannel channel = raf.getChannel();
				ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE/8);
				try {
					channel.position(0);
					channel.read(buffer);
				} catch (IOException e) {
					e.printStackTrace();
				}
				buffer.flip();
				long l = buffer.getLong();
				if (l < writerSyncedTime) {
					try {
						raf.setLength(0);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			}
		}
	}
}
