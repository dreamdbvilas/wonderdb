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
package org.wonderdb.collection.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cache.PrimaryCacheResourceProvider;
import org.wonderdb.cache.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.CollectionTailMgr;
import org.wonderdb.schema.FreeBlocksMgr;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedRecord;
import org.wonderdb.seralizers.block.SerializedRecordLeafBlock;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.RecordValuesType;
import org.wonderdb.types.metadata.impl.SerializedCollectionMetadata;


public class QueriableRecordListImpl implements QueriableRecordList {
	BlockPtr headPtr = null;
//	BlockPtr tailPtr = null;
	int schemaId;
	byte fileId;
	private ReadWriteLock rebalanceLock = new ReentrantReadWriteLock();
	Shard shard = null;
	
	
//	private static CacheHandler<CacheableList> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	
	public QueriableRecordListImpl(Shard shard) {
		this.shard = shard;
		this.schemaId = shard.getSchemaId();
		this.fileId = FileBlockManager.getInstance().getId(shard);
	}
	
	public QueriableRecordListImpl(Shard shard, BlockPtr headPtr) {
		this(shard);
		this.headPtr = headPtr;
//		this.tailPtr = tailPtr;
		this.shard = shard;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.collection.impl.RecordList#getHead(boolean, java.util.Set)
	 */
	@Override
	public RecordBlock getHead(boolean writeLock, List<ColumnType> selectColumns, Set<BlockPtr> pinnedBlocks) {
		if (headPtr == null || (headPtr.getFileId() < 0 && headPtr.getBlockPosn() < 0)) {
			SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
			headPtr = scm.getHead(shard);
		}
		RecordBlock head = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(headPtr, schemaId, pinnedBlocks);
		if (head == null) {
			return null;
		}
		
		if (writeLock) {
			head.writeLock();
		} else {
			head.readLock();
		}
		return head;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.collection.impl.RecordList#add(com.mydreamdb.block.record.DataRecord)
	 */
	@Override
	public RecordBlock add(QueriableBlockRecord record, Set<BlockPtr> pinnedBlocks, TransactionId txnId) {
		boolean shouldUnpin = pinnedBlocks == null;
		if (shouldUnpin) {
			pinnedBlocks = new HashSet<BlockPtr>();
		}
		RecordBlock tailRb = null;
		SerializedRecordLeafBlock tailSrb = null;
		SerializedRecordLeafBlock valueBlock = null;
		BlockPtr returnPtr = null;
		
		rebalanceLock.readLock().lock();
		try {
			SerializedRecord sr = null;
			Map<ColumnType, DBType> map = record.getColumns();
			int maxChunkSize = StorageUtils.getInstance().getMaxChunkSize(fileId);
			int valueSize = estimateValueSize(map);
			int headerSize = estimateHeaderSize(map.size(), valueSize, fileId);
			int totalSize = headerSize + valueSize;
			boolean foundTail = false;
			try {
				while (true) {
					tailRb = CollectionTailMgr.getInstance().getBlock(schemaId, shard, pinnedBlocks);
					tailRb.writeLock();
					foundTail = true;
					try {
						SerializedBlock sb = (SerializedBlock) secondaryCacheHandler.get(tailRb.getPtr());
						tailSrb = new SerializedRecordLeafBlock(sb, false);
						boolean createNew = false;
						boolean useHeaderBlockAsValueBlock = true;
						boolean brandNewBlock = false;
						
						if (headPtr == null) {
							headPtr = tailRb.getPtr();
						}
		
						if (totalSize < tailSrb.getFreeSize()) {
							createNew = false;
							useHeaderBlockAsValueBlock = true;
						} else {
							if (totalSize < maxChunkSize) {
								createNew = true;
								useHeaderBlockAsValueBlock = true;
							} else {
								if (headerSize < tailSrb.getFreeSize()) {
									createNew = false;
									useHeaderBlockAsValueBlock = false;
								} else {
									createNew = true;
									useHeaderBlockAsValueBlock = true;
									brandNewBlock = true;
								}
							}
						}
									
						if (createNew) {
							if (!brandNewBlock) {
								foundTail = false;
								continue;
							} else {
								if (tailRb.getData().size() != 0) {
									foundTail = false;
									continue;
								}
							}
						}
	
						if (useHeaderBlockAsValueBlock) {
							valueBlock = tailSrb;
						} else {
							valueBlock = null;
						}
						break;
					} finally {
						if (!foundTail) {
							tailRb.writeUnlock();
						}
					}
				}
		
				sr = new SerializedRecord(tailSrb, valueBlock, schemaId, pinnedBlocks);
				sr.srbMap.put(tailRb.getPtr(), tailSrb);
				if (valueBlock != null) {
					sr.srbMap.put(valueBlock.getPtr(), valueBlock);
				}
				sr.updateColumns(record.getColumns());
				record.updateLastModifiedDate();
				RecordId recId = sr.getRecordId();
				record.setRecordId(recId);
				
				returnPtr = recId.getPtr();
				if (!returnPtr.equals(tailRb.getPtr())) {
					Logger.getLogger(getClass()).fatal("new record ptr is different from what was expected");
				}
				int blockCount = tailRb.getBlockCount();
				tailRb.addEntry(recId.getPosn(), record);
				int newCount = tailRb.getBlockCount();
				
				if (newCount-blockCount>0) {
					primaryResourceProvider.getResource(tailRb.getPtr(), newCount-blockCount);
				}
				Set<BlockPtr> list = sr.getChangedRecordBlocks();
				Iterator<BlockPtr> iter = list.iterator();
				while (iter.hasNext()) {
					BlockPtr ptr = iter.next();
					if (txnId != null) {
						SerializedBlock sb = (SerializedBlock) secondaryCacheHandler.get(ptr);
						LogManager.getInstance().logBlock(txnId, sb);
					}
					secondaryCacheHandler.changed(ptr);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
			}
			return tailRb;
		} finally {
			tailRb.writeUnlock();
			rebalanceLock.readLock().unlock();
			if (shouldUnpin) {
				CacheEntryPinner.getInstance().unpin(tailRb.getPtr(), pinnedBlocks);
				unpin(pinnedBlocks);
			}
			CollectionTailMgr.getInstance().returnTailPtr(schemaId, shard, returnPtr);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.collection.impl.RecordList#update(java.util.Map, com.mydreamdb.block.record.DataRecord, com.mydreamdb.block.record.RecordBlock)
	 */
	@Override
	public void update(RecordId recordId, QueriableBlockRecord record, Set<BlockPtr> pinnedBlocks, TransactionId txnId) {
		rebalanceLock.readLock().lock();
		try {
			int posn = recordId.getPosn();
			Map<ColumnType, DBType> changedColumns = record.getColumns();
//			Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
			List<ColumnType> columns = new ArrayList<ColumnType>(changedColumns.keySet());
//			BlockRecordAndSerializedRecord brsr = CacheObjectMgr.getInstance().getRecordWithSerializedRecord(recordId, columns, schemaId, pinnedBlocks);
//			RecordBlock t = (RecordBlock) primaryCacheHandler.get(recordId.getPtr());
			RecordBlock t = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedBlocks);
			if (t == null) {
				return;
			}
			SerializedRecord sr = null;
			try {
				t.writeLock();
				
				SerializedBlock ref = null;
				CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
				ref = (SerializedBlock) secondaryCacheHandler.get(recordId.getPtr());
				
				SerializedBlock serializedBlock = ref;
				SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(serializedBlock, false);
				
				sr = new SerializedRecord(srb, schemaId, pinnedBlocks);
				sr.loadData(recordId, columns);
				sr.updateColumns(changedColumns);
				QueriableBlockRecord rec = t.getRecordData(posn);
//				int currentSize = estimateValueSize(rec.getColumns());
				int currentCount = t.getBlockCount();
				rec.setColumns(changedColumns);
				int newCount = t.getBlockCount();
				if (newCount-currentCount > 0) {
					primaryResourceProvider.getResource(t.getPtr(), newCount-currentCount);
				}
				rec.updateLastModifiedDate();
				int newSize = sr.getLoadedBufferCount();
//				primaryResourceProvider.getResource(recordId.getPtr(), newSize-currentSize);
				rec.setBufferCount(newSize);
				
				Set<BlockPtr> list = sr.getChangedRecordBlocks();
				Iterator<BlockPtr> iter = list.iterator();
				while (iter.hasNext()) {
					BlockPtr ptr = iter.next();
					if (txnId != null) {
						SerializedBlock sb = (SerializedBlock) secondaryCacheHandler.get(ptr);
						LogManager.getInstance().logBlock(txnId, sb);
					}

					secondaryCacheHandler.changed(ptr);
				}

			} finally {
				t.writeUnlock();
//				unpin(pinnedBlocks);
			}
		} finally {
			rebalanceLock.readLock().unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.collection.impl.RecordList#delete(com.mydreamdb.block.record.DataRecord, com.mydreamdb.block.record.RecordBlock)
	 */
	@Override
	public void delete(RecordId recordId, Set<BlockPtr> pinnedBlocks, TransactionId txnId) {
		SerializedRecord sr = null;
//		BlockPtr removeBlockPtr = null;
//		
//		BlockPtr nextBlockPtr = null;
//		BlockPtr prevBlockPtr = null;
//		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		
		rebalanceLock.readLock().lock();
		try {
			RecordBlock recordBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedBlocks);
			if (recordBlock == null) {
				return;
			}
			recordBlock.writeLock();
			
			int posn = binarySearch(recordBlock.getData(), recordId.getPosn());
			
			try {
				if (posn < 0) {
					return;
				}
				int currentSize = recordBlock.getBlockCount();
				QueriableBlockRecord qbr = (QueriableBlockRecord) recordBlock.getData().remove(posn);
				int newSize = recordBlock.getBlockCount();
				if (newSize < currentSize) {
					primaryResourceProvider.returnResource(recordBlock.getPtr(), currentSize-newSize);
				}
				PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider().returnResource(recordId.getPtr(), qbr.getBufferCount());
				SerializedBlock ref = null;
				CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
				ref = (SerializedBlock) secondaryCacheHandler.get(recordId.getPtr());
				
				SerializedBlock serializedBlock = ref;
				SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(serializedBlock, false);
	
				sr = new SerializedRecord(srb, schemaId, pinnedBlocks);
				if (sr.loadData(recordId, null)) {
//					int size = sr.getLoadedBufferCount();
//					primaryResourceProvider.returnResource(recordId.getPtr(), size);
					sr.remove();
				}
				
//				if (recordBlock.getData().size() == 0) {
//					// we need to remove it.
//					removeBlockPtr = recordBlock.getPtr();
//					nextBlockPtr = recordBlock.getNext();
//					prevBlockPtr = recordBlock.getPrev();
//				}
				Set<BlockPtr> list = sr.getChangedRecordBlocks();
				Iterator<BlockPtr> iter = list.iterator();
				while (iter.hasNext()) {
					BlockPtr ptr = iter.next();
					if (txnId != null) {
						SerializedBlock sb = (SerializedBlock) secondaryCacheHandler.get(ptr);
						LogManager.getInstance().logBlock(txnId, sb);
					}

					secondaryCacheHandler.changed(ptr);
				}
			} finally {
				recordBlock.writeUnlock();
			}
		} finally {
			rebalanceLock.readLock().unlock();
		}
		
//		if (removeBlockPtr != null) {
//			rebalanceLock.writeLock().lock();
//			try {
//				removeBlock(removeBlockPtr, prevBlockPtr, nextBlockPtr, pinnedBlocks);
//			} finally {
//				rebalanceLock.writeLock().unlock();
//			}
//		}
	}
	
	@SuppressWarnings("unused")
	public void removeBlock(BlockPtr removeBlockPtr) {
		BlockPtr prevBlockPtr = null;
		BlockPtr nextBlockPtr = null;
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		RecordBlock prevBlock = null;
		RecordBlock nextBlock = null;
		RecordBlock removeBlock = null;
		TransactionId txnId = LogManager.getInstance().startTxn();
		
		rebalanceLock.writeLock().lock();
		try {
			removeBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(removeBlockPtr, schemaId, pinnedBlocks);
			if (prevBlockPtr == null && nextBlockPtr == null) {
				return;
			}
			
			if (removeBlock.getData().size() > 0) {
				return;
			}
			
			prevBlockPtr = removeBlock.getPrev();
			nextBlockPtr = removeBlock.getNext();
			
			if (prevBlockPtr != null && nextBlockPtr == null) {
				CacheEntryPinner.getInstance().pin(prevBlockPtr, pinnedBlocks);
				prevBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(prevBlockPtr, schemaId, pinnedBlocks);
				SerializedBlock ref = null;
				ref = (SerializedBlock) secondaryCacheHandler.get(prevBlockPtr);
				SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(ref, false);
				srb.setNextPtr(null);
				LogManager.getInstance().logBlock(txnId, ref);
			}
			
			if (prevBlockPtr == null && nextBlockPtr != null) {
				CacheEntryPinner.getInstance().pin(nextBlockPtr, pinnedBlocks);
				nextBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(nextBlockPtr, schemaId, pinnedBlocks);
				headPtr = nextBlock.getPtr();
				updateHead(shard);
			}
			
			if (prevBlockPtr != null && nextBlockPtr != null) {
				CacheEntryPinner.getInstance().pin(prevBlockPtr, pinnedBlocks);
				CacheEntryPinner.getInstance().pin(nextBlockPtr, pinnedBlocks);
				prevBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(prevBlockPtr, schemaId, pinnedBlocks);
				nextBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(nextBlockPtr, schemaId, pinnedBlocks);
				SerializedBlock ref = null;
				ref = (SerializedBlock) secondaryCacheHandler.get(prevBlockPtr);
				SerializedRecordLeafBlock srb = new SerializedRecordLeafBlock(ref, false);
				srb.setNextPtr(nextBlockPtr);
				LogManager.getInstance().logBlock(txnId, ref);

				ref = (SerializedBlock) secondaryCacheHandler.get(nextBlockPtr);
				srb = new SerializedRecordLeafBlock(ref, false);
				srb.setPrevPtr(prevBlockPtr);					
				LogManager.getInstance().logBlock(txnId, ref);
			}
			
			FreeBlocksMgr.getInstance().add(schemaId, removeBlockPtr);
			LogManager.getInstance().commitTxn(txnId);
		} finally {
			rebalanceLock.writeLock().unlock();
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	private int binarySearch(CacheableList list, int posn) {
		int retVal = 0;	
		boolean found = false;
		
		for (;retVal < list.size(); retVal++) {
			QueriableBlockRecord e = (QueriableBlockRecord) list.get(retVal);
			if (posn == e.getRecordId().getPosn()) {
				found = true;
				break;
			} else if (e.getRecordId().getPosn() > posn) {
				break;
			}
		}
		if (found) {
			return retVal;
		}
		return -1;
	}
	
	public void setHeadPtr(BlockPtr ptr) {
		headPtr = ptr;
	}

	private void unpin(Set<BlockPtr> pinnedBlocks) {
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
	}
	
	private int estimateHeaderSize(int noOfColumns, int valueSize, byte fileId) {
		int overhead = StorageUtils.getInstance().getOverhead(fileId) + SerializedRecordLeafBlock.HEADER_SIZE;
		int headerSize = overhead + (noOfColumns * ((BlockPtrSerializer.BASE_SIZE + Integer.SIZE/8) + Integer.SIZE/8)) + (Integer.SIZE/8 * noOfColumns) + Integer.SIZE/8;
		return headerSize;
	}
	
	private int estimateValueSize(Map<ColumnType, ? extends SerializableType> map) {
		Collection<? extends SerializableType> collection = map.values();
		List<SerializableType> list = new ArrayList<SerializableType>(collection);
		RecordValuesType rvt = new RecordValuesType(list);
		int overhead = StorageUtils.getInstance().getOverhead(fileId);
		int valueSize = rvt.getByteSize();
		int maxChunkSize = StorageUtils.getInstance().getMaxChunkSize(fileId);
		int blocksRequired = (valueSize / maxChunkSize) + 1;
		blocksRequired = Math.max(blocksRequired, map.size());
		return (blocksRequired * overhead) + rvt.getByteSize();
	}	
	
	
	private void updateHead(Shard shard) {
		TransactionId txnId = LogManager.getInstance().startTxn();
		try {
			SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
			scm.updateHead(shard, headPtr, txnId);
		} finally {
			LogManager.getInstance().commitTxn(txnId);
		}

	}

	@Override
	public Shard getShard() {
		return shard;
	}
}