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
package org.wonderdb.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.schema.FileBlockEntryType;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.server.WonderDBServer;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.StringType;



public class FileBlockManager {
	public static final int COLLECTION_BLOCK = 1;
	public static final int INDEX_BLOCK = 2;
	
	private static int POOLED_POINTERS_SIZE_HIGH_WATERMARK = 5;
	private static int POOLED_POINTERS_SIZE_LOW_WATERMARK = 2;
	
//	public static int BLOCK_SIZE = 2048;
	private static FileBlockManager instance = new FileBlockManager();
	Map<String, FileBlockEntryType> fileBlockMap = new HashMap<String, FileBlockEntryType>();
	Map<Byte, FileBlockEntryType> fileIdToBlockMap = new HashMap<Byte, FileBlockEntryType>();
	Map<String, BlockingQueue<Long>> pointerPoolMap = new HashMap<String, BlockingQueue<Long>>();
	
	byte defaultFileId = -1;
//	byte nextFileId = 0;
	
	int smallestBlockSize = WonderDBServer.DEFAULT_BLOCK_SIZE;
	
	private FileBlockManager() {
	}
	
	public static FileBlockManager getInstance() {
		return instance;
	}
	
	public List<FileBlockEntryType> getFileBlockEntries() {
		return new ArrayList<FileBlockEntryType>(fileBlockMap.values());
	}
		
	public FileBlockEntryType addFileBlockEntry(FileBlockEntryType entry) {
		if (!fileBlockMap.containsKey(entry.getFileName())) {
			try {
				FilePointerFactory.getInstance().getSize(entry.getFileName());
			} catch (Exception e) {
				throw new StorageFileCreationException();
			}
			fileBlockMap.put(entry.getFileName(), entry);
			if (entry.getFileId() < 0) {
				entry.setFileId((byte) (fileBlockMap.size()-1));
			}
			fileIdToBlockMap.put(entry.getFileId(), entry);
			if (entry.isDefault() || defaultFileId < 0) {
				defaultFileId = entry.getFileId();
			}
			
			BlockingQueue<Long> bq = new ArrayBlockingQueue<Long>(100);
			pointerPoolMap.put(entry.getFileName(), bq);
			return entry;
		}
		return fileBlockMap.get(entry.getFileName());
	}
	
	public int getSmallestBlockSize() {
		return smallestBlockSize;
	}
	
	public long getNextBlock(Shard shard) {
		return getNextBlock(getFileName(shard));
	}
	
	public String getFileName(Shard shard) {
		String fileName = "";
		if (shard.getSchemaId() < 3) {
			fileName = WonderDBPropertyManager.getInstance().getSystemFile();
		} else {
			fileName = shard.getSchemaObjectName() + "_" + shard.getReplicaSetName();
		}
		
		return fileName;

	}
	
	public String getFileName(BlockPtr ptr) {
		FileBlockEntryType entry = fileIdToBlockMap.get(ptr.getFileId());
		return entry.getFileName();
	}
	
	private long getNextBlock(String fileName) {
		BlockingQueue<Long> bq = pointerPoolMap.get(fileName);
		long curPosn = -1;
		if (bq.size() <= POOLED_POINTERS_SIZE_LOW_WATERMARK) {
			FileBlockEntryType entry = fileBlockMap.get(fileName);
			synchronized (entry) {				
				for (int i = bq.size(); i < POOLED_POINTERS_SIZE_HIGH_WATERMARK; i++) {
					long posn = entry.addBy(entry.getBlockSize());
					bq.add(posn);
				}
				updateCurrentPosn(entry);				
			}
		}
		try { 
			while (true) {
				curPosn = bq.take();
				if ("system".equals(fileName) && curPosn < WonderDBServer.DEFAULT_BLOCK_SIZE*3) {
					continue;
				} else {
					break;
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		if (bq.size() <= POOLED_POINTERS_SIZE_LOW_WATERMARK) {
			FileBlockEntryType entry = fileBlockMap.get(fileName);
			synchronized (entry) {				
				for (int i = bq.size(); i < POOLED_POINTERS_SIZE_HIGH_WATERMARK; i++) {
					long posn = entry.addBy(entry.getBlockSize());
					bq.add(posn);
				}
				updateCurrentPosn(entry);				
			}
		}
		return curPosn;
//		FileBlockEntryType entry = fileBlockMap.get(fileName);	
//		Long curPosn = null;
//		synchronized (entry) {
////			curPosn = entry.getCurrentPosn();
//			curPosn = entry.addBy(entry.getBlockSize());
//			long s = FilePointerFactory.getInstance().getSize(fileName);
//			if (s < curPosn + entry.getBlockSize()) {
//				FilePointerFactory.getInstance().doubleSize(fileName);
//			}
//			Map<String, DBType> map = new HashMap<String, DBType>();
//			map.put("currentFilePosn", new StringType(String.valueOf(entry.getCurrentPosn())));
//			try {
//				TableRecordManager.getInstance().updateTableRecord("fileStorageMetaCollection", map, entry.getRecordId(), null);
//			} catch (InvalidCollectionNameException e) {
//				e.printStackTrace();
//			}				
//		}
//		return curPosn;
	}	
	
	public long getNextBlock(BlockPtr ptr) {
		byte fileId = ptr.getFileId();
		FileBlockEntryType entry = fileIdToBlockMap.get(fileId);
		return getNextBlock(entry.getFileName());
	}
	
	private void updateCurrentPosn(FileBlockEntryType entry) {
		synchronized (entry) {
//			curPosn = entry.getCurrentPosn();
			long curPosn = entry.getCurrentPosn();
			long s = FilePointerFactory.getInstance().getSize(entry.getFileName());
			if (s < curPosn + entry.getBlockSize()) {
				FilePointerFactory.getInstance().doubleSize(entry.getFileName());
			}
			Map<String, DBType> map = new HashMap<String, DBType>();
			map.put("currentFilePosn", new StringType(String.valueOf(entry.getCurrentPosn())));
			TransactionId txnId = LogManager.getInstance().startTxn();
			try {
				Shard shard = new Shard(0, "", "");
				TableRecordManager.getInstance().updateTableRecord("fileStorageMetaCollection", map, shard, entry.getRecordId(), null, txnId);
			} catch (InvalidCollectionNameException e) {
				e.printStackTrace();
			} finally {
				LogManager.getInstance().commitTxn(txnId);
			}
		}
	}
	
//	public long getNextBlock(byte fileId) {
//		return getNextBlock(getFileName(fileId));
//	}
	
	public int getBlockSize(String fileName) {
		FileBlockEntryType et = fileBlockMap.get(fileName);
		if (et == null) {
			return smallestBlockSize;
		}
		return et.getBlockSize();
	}
	
	public int getBlockSize(byte fileId) {
		FileBlockEntryType et = fileIdToBlockMap.get(fileId);
		if (et == null) {
			return smallestBlockSize;
		}
		return et.getBlockSize();		
	}
	
	public String getDefaultFileName() {
		FileBlockEntryType entry = fileIdToBlockMap.get(defaultFileId);
		if (entry == null) {
			return null;
		}
		
		return entry.getFileName();
	}
	
	public byte getId(Shard shard) {
		FileBlockEntryType entry = fileBlockMap.get(getFileName(shard));
		if (entry == null) {
			return -1;
		}
		return entry.getFileId();
	}
	
//	public String getFileName(byte id) {
//		FileBlockEntryType entry = fileIdToBlockMap.get(id);
//		if (entry == null) {
//			return null;
//		}
//		
//		return entry.getFileName();
//	}
	
	public FileBlockEntryType getEntry(Shard shard) {
		return fileBlockMap.get(getFileName(shard));
	}
	
	public FileBlockEntryType getMetaEntry(String fileName) {
		return fileBlockMap.get(fileName);
	}
}
