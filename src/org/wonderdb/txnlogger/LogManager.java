package org.wonderdb.txnlogger;

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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.impl.CacheWriter;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.types.BlockPtr;

public class LogManager {
	AtomicInteger at = new AtomicInteger();
	Set<Integer> committingTxns = new HashSet<Integer>();
	Map<BlockPtr, SerializedBlockImpl> blockPtrBlockMap = new HashMap<BlockPtr, SerializedBlockImpl>();
	
	Map<Integer, Set<BlockPtr>> blockPtrsInTxn = new HashMap<Integer, Set<BlockPtr>>();
	
	List<RandomAccessFile> logFiles = new ArrayList<RandomAccessFile>();
	int currentFilePosn = 0;
	
	CacheWriter<BlockPtr, ChannelBuffer> writer = null;
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
			if (!file.exists()) {
				try {
					file.createNewFile();
					RandomAccessFile raf = new RandomAccessFile(file, "rw");
					logFiles.add(raf);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
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
		Set<BlockPtr> ptrList = new HashSet<BlockPtr>();
		blockPtrsInTxn.put(txnId.getId(), ptrList);
		return txnId;
	}
	
	public synchronized void logBlock(TransactionId txnId, SerializedBlockImpl block) {
		if (!loggingEnabled) {
			return;
		}
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(block.getFullBuffer());
		SerializedBlockImpl sb = new SerializedBlockImpl(block.getPtr(), buffer);
		blockPtrBlockMap.put(block.getPtr(), sb);
		sb.setLastAccessTime(block.getLastAccessTime());
		Set<BlockPtr> ptrList = blockPtrsInTxn.get(txnId.getId());
		ptrList.add(block.getPtr());		
	}
	
	public synchronized void commitTxn(TransactionId txnId) {
		if (!loggingEnabled) {
			return;
		}
		if (txnId == null) {
			return;
		}
		committingTxns.add(txnId.getId());
		logTransaction(txnId.getId());			
	}
	
	private boolean shouldWait(int txnId, Set<Integer> dependetTxns) {
		Set<BlockPtr> ptrList = blockPtrsInTxn.get(txnId);
		Iterator<Integer> iter = blockPtrsInTxn.keySet().iterator();
		while (iter.hasNext()) {
			int txn = iter.next();
			if (txn == txnId) {
				continue;
			}
			
			Set<BlockPtr> list = blockPtrsInTxn.get(txn);
			Iterator<BlockPtr> txnPtrIter = ptrList.iterator();
			
			while (txnPtrIter.hasNext()) {
				BlockPtr ptr = txnPtrIter.next();
				if (list.contains(ptr) && !committingTxns.contains(txn)) {
					dependetTxns.clear();
					return true;
				}

				if (list.contains(ptr) && committingTxns.contains(txn)) {
					dependetTxns.add(txn);					
				}
			}
		}
		
		return false;
	}
	
	private synchronized void logTransaction(int txnId) {
		Set<Integer> dependentTxns = new HashSet<Integer>();
		while (true) {
			if (!committingTxns.contains(txnId)) {
				return;
			}
			if (shouldWait(txnId, dependentTxns)) {
				try {
					wait();
				} catch (InterruptedException e) {
					continue;
				}
			} else {
				break;
			}
		}
		
		Set<BlockPtr> bpList = new HashSet<BlockPtr>();
		bpList.addAll(blockPtrsInTxn.remove(txnId));
		
		Iterator<Integer> iter = dependentTxns.iterator();
		while (iter.hasNext()) {
			int txn = iter.next();
			bpList.addAll(blockPtrsInTxn.remove(txn));

		}
		
		Set<BlockPtr> bpSet = new HashSet<BlockPtr>(bpList);
		
		List<SerializedBlockImpl> sblist = new ArrayList<SerializedBlockImpl>();
		Iterator<BlockPtr> iter1 = bpSet.iterator();
		
		while (iter1.hasNext()) {
			SerializedBlockImpl sb = blockPtrBlockMap.remove(iter1.next()); 
			sblist.add(sb);
		}
		
		write(sblist);
		committingTxns.remove(txnId);
		committingTxns.removeAll(dependentTxns);
		notifyAll();
	}	
	
	private void write(List<SerializedBlockImpl> list) {
		if (list == null || list.size() == 0) {
			return;
		}
		ChannelBuffer[] buffers = new ChannelBuffer[list.size()];
		for (int i = 0; i < list.size(); i++) {
			SerializedBlockImpl sb = list.get(i);
			buffers[i] = sb.getFullBuffer();
			currentMaxTxnTime = Math.max(currentMaxTxnTime, sb.getLastAccessTime());
		}
		
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buffers);		
		ChannelBuffer header = ChannelBuffers.buffer((Integer.SIZE/8)*2);
		header.writeInt(buffer.capacity());
		header.writeInt(buffer.hashCode());

		ChannelBuffer wrapped = ChannelBuffers.wrappedBuffer(header, buffer);
		FileChannel channel = getFileChannel(buffer.capacity());
		ByteBuffer buf = wrapped.toByteBuffer();
		
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
