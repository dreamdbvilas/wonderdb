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
package org.wonderdb.cache;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.impl.disk.DiskRecordBlock;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.index.impl.base.BaseIndexBlock;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.IndexBlockSerializer;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedContinuationBlock;
import org.wonderdb.seralizers.block.SerializedIndexBranchContinuationBlock;
import org.wonderdb.seralizers.block.SerializedIndexLeafContinuationBlock;
import org.wonderdb.seralizers.block.SerializedRecord;
import org.wonderdb.seralizers.block.SerializedRecordChunk;
import org.wonderdb.seralizers.block.SerializedRecordLeafBlock;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;


public class CacheObjectMgr {
	ConcurrentMap<BlockPtr, BlockPtr> inflightRead = new ConcurrentHashMap<BlockPtr, BlockPtr>();
	
	private static CacheObjectMgr instance = new CacheObjectMgr();
	
	private static CacheHandler<CacheableList> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();
//	private static CacheResourceProvider<SerializedBlock> secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	
	private CacheObjectMgr() {
	}
	
	public static CacheObjectMgr getInstance() {
		return instance;
	}
	
	public IndexBlock getIndexBlockFromSecondaryCache(BlockPtr ptr, int schemaId, Set<BlockPtr> pinnedBlocks) {
		SerializedBlock ref = null;
		ref = (SerializedBlock) secondaryCacheHandler.get(ptr);
		synchronized (ref) {
			SerializedBlock serializedBlock = ref;
			SerializedContinuationBlock serializedIndexBlock = null;
			IndexBlock indexBlock = null;
			ChannelBuffer buffer = null;
			if (SerializedBlock.BRANCH_BLOCK == serializedBlock.getBlockType()) {
				serializedIndexBlock = new SerializedIndexBranchContinuationBlock(serializedBlock, pinnedBlocks, false);
				buffer = serializedIndexBlock.getDataBuffer();
				indexBlock = IndexBlockSerializer.getInstance().unmarshalBranchBlock(ptr, buffer);
			} else if (SerializedBlock.INDEX_LEAF_BLOCK == serializedBlock.getBlockType()) {
				serializedIndexBlock = new SerializedIndexLeafContinuationBlock(serializedBlock, pinnedBlocks, false);
				buffer = serializedIndexBlock.getDataBuffer();
				indexBlock = IndexBlockSerializer.getInstance().unmarshalLeafBlock(ptr, buffer, schemaId);
			} else {
				throw new RuntimeException("invalid block type");
			}
			indexBlock.setBufferCount(serializedIndexBlock.getBlocks().size());
			primaryResourceProvider.getResource(ptr, serializedIndexBlock.getBlocks().size());
			CacheEntryPinner.getInstance().pin(indexBlock.getPtr(), pinnedBlocks);
			primaryCacheHandler.addIfAbsent(indexBlock);
			return indexBlock;		
		}
	}
	
	public IndexBlock getIndexBlock(BlockPtr ptr, int schemaId, Set<BlockPtr> pinnedBlocks) {
		
		if (ptr == null || ptr.getFileId() < 0 || ptr.getBlockPosn() < 0) {
			return null;
		}
		
		IndexBlock indexBlock = null;
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		
//		try {
			indexBlock = (IndexBlock) primaryCacheHandler.get(ptr);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		if (indexBlock != null) {
			return indexBlock;
		}

		BlockPtr p = inflightRead.putIfAbsent(ptr, ptr);
		if (p == null) {
			indexBlock = getIndexBlockFromSecondaryCache(ptr, schemaId, pinnedBlocks);
			inflightRead.remove(ptr);
			synchronized (ptr) {
				ptr.notifyAll();
			}
		} else {
			synchronized (p) {
				while (indexBlock == null) {
					try {
						p.wait(10);
					} catch (InterruptedException e) { }
					indexBlock = (IndexBlock) primaryCacheHandler.get(ptr);
				}
			}
		}
		((BaseIndexBlock) indexBlock).setSchemaObjectId(schemaId);
		return indexBlock;
	}
	
//	public void unpin(BlockPtr ptr, Set<BlockPtr> pinnedBlocks) {
//		CacheEntryPinner.getInstance().unpin(ptr, pinnedBlocks);
//	}
	
	public SerializedContinuationBlock updateIndexBlock(IndexBlock block, int schemaId, Set<BlockPtr> pinnedBlocks) {
//		CacheEntryPinner.getInstance().pin(block.getPtr(), pinnedBlocks);
		
		CacheEntryPinner.getInstance().pin(block.getPtr(), pinnedBlocks);
//		primaryCacheHandler.addIfAbsent(block);
		SerializedBlock serializedBlock = (SerializedBlock) secondaryCacheHandler.get(block.getPtr());
		SerializedContinuationBlock serializedIndexBlock = null;
		
		if (block instanceof IndexBranchBlock) {
			serializedIndexBlock = new SerializedIndexBranchContinuationBlock(serializedBlock, pinnedBlocks, false);
		} else {
			serializedIndexBlock = new SerializedIndexLeafContinuationBlock(serializedBlock, pinnedBlocks, false);
		}
		
		ChannelBuffer buffer = serializedIndexBlock.getDataBuffer(block.getByteSize());
		IndexBlockSerializer.getInstance().toBytes(block, buffer, schemaId);
		int currentSize = block.getBufferCount();
		
		block.setBufferCount(serializedIndexBlock.getBufferCount());
		primaryResourceProvider.getResource(block.getPtr(), block.getBufferCount()-currentSize);
		return serializedIndexBlock;
	}
	
	private List<ColumnType> getColumnsToLoad(Map<ColumnType, DBType> loadedColumns, List<ColumnType> loadColumns, int schemaId) {
		if (schemaId < 3) {
			return null;
		}
		
		if (loadedColumns.isEmpty() && loadColumns == null) {
			return null;
		}
		
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
		List<ColumnType> list = new ArrayList<ColumnType>();
		for (int i = 0; i < loadColumns.size(); i++) {
			ColumnType ct = loadColumns.get(i);
			if (ct == null) {
				continue;
			}
			if (!loadedColumns.containsKey(ct) && ct.getValue() instanceof Integer && colMeta.getCollectionColumn((Integer) ct.getValue()) != null ) {
				list.add(ct);
			}
		}
		
		return list;
	}
	
	@SuppressWarnings("unused")
	public QueriableBlockRecord getRecord(RecordBlock lockedBlock, 
			RecordId recordId, List<ColumnType> loadColumns, int schemaId, Set<BlockPtr> pinnedBlocks) {
		Set<BlockPtr> pinnedSet = pinnedBlocks;
		boolean unpinBlocks = false;
		
		if (pinnedSet == null) {
			pinnedSet = new HashSet<BlockPtr>();
			unpinBlocks = true;
		}
		try {
			SerializedRecord sr = null;
			RecordBlock block = getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedSet);
			if (block == null) {
				return null;
			}
			int blockCount = block.getBlockCount();
			int currentSize = getSize(block);
			QueriableBlockRecord record = null;
			try {
				record = block.getRecordData(recordId.getPosn());
			} catch (Exception e) {
				e.printStackTrace();
			}
			record.setRecordId(recordId);
			Map<ColumnType, DBType> loadedColumns = record.getColumns();
			List<ColumnType> columnsToLoad = null;
			List<ColumnType> allColumns = new ArrayList<ColumnType>(SchemaMetadata.getInstance().getCollectionMetadata(schemaId).getQueriableColumns().keySet());
			if (loadColumns == null) {
				columnsToLoad = allColumns;
			} else {
				columnsToLoad = loadColumns;
			}
			List<ColumnType> columnsToLoadList = getColumnsToLoad(loadedColumns, columnsToLoad, schemaId);
			if (columnsToLoadList == null || columnsToLoadList.size() > 0) {
				SerializedBlock ref = null;
				CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedSet);
				ref = (SerializedBlock) secondaryCacheHandler.get(recordId.getPtr());
				synchronized (ref) {
					SerializedBlock serializedBlock = ref;
					SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(serializedBlock, false);
		
					sr = new SerializedRecord(srb, schemaId, pinnedSet);
					if (sr.loadData(recordId, columnsToLoadList)) {
						putInDataRecord(record, sr, columnsToLoadList);
						record.updateLastAccessDate();
		//				return record;
					}
					record.setBufferCount(sr.getLoadedBufferCount());
				}
//				primaryResourceProvider.getResource(recordId.getPtr(), sr.getLoadedBufferCount());
			}
			int newBlockCount = block.getBlockCount();
			if ( newBlockCount-blockCount > 0) {
				primaryResourceProvider.getResource(recordId.getPtr(), newBlockCount-blockCount);
			}
			return record;
		} finally {
			if (unpinBlocks) {
				CacheEntryPinner.getInstance().unpin(pinnedSet, pinnedSet);
			}
		}
	}
	
	public BlockRecordAndSerializedRecord getRecordWithSerializedRecord(RecordId recordId, List<ColumnType> loadColumns, int schemaId, Set<BlockPtr> pinnedBlocks) {
		Set<BlockPtr> pinnedSet = pinnedBlocks;
		boolean unpinBlocks = false;
		
		if (pinnedSet == null) {
			pinnedSet = new HashSet<BlockPtr>();
			unpinBlocks = true;
		}
		try {
			SerializedRecord sr = null;
			RecordBlock block = getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedSet);
			if (block == null) {
				return null;
			}
			QueriableBlockRecord record = block.getRecordData(recordId.getPosn());
			record.setRecordId(recordId);
			List<ColumnType> columnsToLoad = null;
			List<ColumnType> allColumns = new ArrayList<ColumnType>(SchemaMetadata.getInstance().getCollectionMetadata(schemaId).getQueriableColumns().keySet());
			if (loadColumns == null) {
				columnsToLoad = allColumns;
			} else {
				columnsToLoad = loadColumns;
			}
			List<ColumnType> columnsToLoadList = columnsToLoad;
			if (columnsToLoadList == null || columnsToLoadList.size() > 0) {
				SerializedBlock ref = null;
				CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedSet);
				ref = (SerializedBlock) secondaryCacheHandler.get(recordId.getPtr());
				synchronized (ref) {
					SerializedBlock serializedBlock = ref;
					SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(serializedBlock, false);
		
					sr = new SerializedRecord(srb, schemaId, pinnedSet);
					if (sr.loadData(recordId, columnsToLoadList)) {
						putInDataRecord(record, sr, columnsToLoadList);
						record.updateLastAccessDate();
		//				return record;
					}
				}
				record.setBufferCount(sr.getLoadedBufferCount());
//				primaryResourceProvider.getResource(recordId.getPtr(), sr.getLoadedBufferCount());
			}
			BlockRecordAndSerializedRecord brsr = new BlockRecordAndSerializedRecord();
			brsr.qbr = record;
			brsr.sr = sr;
			return brsr;
		} finally {
			if (unpinBlocks) {
				CacheEntryPinner.getInstance().unpin(pinnedSet, pinnedSet);
			}
		}
	}

//	public RecordBlock loadRecordAndGetBlock(RecordId recordId, List<ColumnType> loadColumns, int schemaId, Set<BlockPtr> pinnedBlocks) {
//		getRecord(recordId, loadColumns, schemaId, pinnedBlocks);
//		RecordBlock block = null;
//		CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
//		block = (RecordBlock) primaryCacheHandler.get(recordId.getPtr());
//		if (block != null) {
//			return block;
//		}
//		return null;
//	}
//	
	public RecordBlock getRecordBlockWithHeaderLoaded(BlockPtr ptr, int schemaId, Set<BlockPtr> pinnedBlocks) {
		if (ptr == null) {
			return null;
		}
		RecordBlock block = null;
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		block = (RecordBlock) primaryCacheHandler.get(ptr);
		if (block != null) {
			return block;
		}
		
		BlockPtr p = inflightRead.putIfAbsent(ptr, ptr);
		if (p == null) {
			try {
				block = getRecordBlockFromSecondaryCache(ptr, schemaId, pinnedBlocks);
			} finally {
				inflightRead.remove(ptr);
			}
			synchronized (ptr) {
				ptr.notifyAll();				
			}
		} else {
			synchronized (p) {
				while (true) {
					try {
						p.wait();
						break;
					} catch (InterruptedException e) {
					}
				}
			}
			block = (RecordBlock) primaryCacheHandler.get(ptr);
		}
		return block;
	}
	
	private int getSize(RecordBlock block) {
		int size = 0;
		CacheableList list = block.getData();
		for (int i = 0; i < list.size(); i++) {
			QueriableBlockRecord record = (QueriableBlockRecord) list.get(i);
			size = size + record.getByteSize();
		}
		return size;
	}
	
	@SuppressWarnings("unused")
	private RecordBlock getRecordBlockFromSecondaryCache(BlockPtr ptr, int schemaId, Set<BlockPtr> pinnedBlocks) {
		SerializedBlock serializedBlock = null;
		serializedBlock = (SerializedBlock) secondaryCacheHandler.get(ptr);
//		synchronized (serializedBlock) {
			SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(serializedBlock, false);
			List<SerializedRecordChunk> chunkList = srb.getContentList();
			if (chunkList == null) {
				return null;
			}
			RecordBlock recordBlock = new DiskRecordBlock(schemaId, ptr);
			primaryResourceProvider.getResource(ptr, 1);
			CacheEntryPinner.getInstance().pin(recordBlock.getPtr(), pinnedBlocks);
			for (SerializedRecordChunk chunk : chunkList) {
				if (chunk.getChunkType() == SerializedRecordChunk.HEADER_CHUNK) {
					RecordId recordId = new RecordId(recordBlock.getPtr(), chunk.getRecordPosn());
					TableRecord record = new TableRecord(recordId, null);
					recordBlock.addEntry(chunk.getRecordPosn(), record);
					recordBlock.setNext(srb.getNextPtr());
					recordBlock.setPrev(srb.getPrevPtr());
				} 
			}
			Object o = primaryCacheHandler.addIfAbsent(recordBlock);
			if (o != recordBlock) {
				int x = 0;
			}
			return recordBlock;				
//		}
	}
	
	private void putInDataRecord(QueriableBlockRecord record, SerializedRecord sr, List<ColumnType> columnsToLoad) {
		if (columnsToLoad == null) {
			record.setColumns(sr.getAllColumns());
			return;
		} 
		int size = 0;
		for (int i = 0; i < columnsToLoad.size(); i++) {
			ColumnType ct = columnsToLoad.get(i);
			DBType value = (DBType) sr.getValue(ct);
			record.setColumnValue(ct, value);
			size = size + (value != null ? value.getByteSize() : 0);
		}
//		primaryResourceProvider.getResource(size);
	}	
	
	public static class BlockRecordAndSerializedRecord {
		public QueriableBlockRecord qbr = null;
		public SerializedRecord sr = null;
	}
}
