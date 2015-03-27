package org.wonderdb.core.collection.impl;

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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockEntryPosition;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexBlock;
import org.wonderdb.block.IndexBranchBlock;
import org.wonderdb.block.IndexCompareIndexQuery;
import org.wonderdb.block.IndexLeafBlock;
import org.wonderdb.block.IndexQuery;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.collection.exceptions.UniqueKeyViolationException;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.RecordUtils;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.freeblock.FreeBlockFactory;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.ObjectRecord;


public class BTreeImpl implements BTree {
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();

	BlockPtr head = null;
	BlockPtr treeHead = null;
	BlockPtr root = null;
	public ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	boolean unique = false;
	TypeMetadata meta;
	
	private BTreeImpl(boolean unique, BlockPtr head, BlockPtr treeHead, BlockPtr root, TypeMetadata meta) {
		if (root != null && root.getFileId() >= 0 && root.getBlockPosn() >= 0) {
			this.root = root;
		}
		if (head != null && head.getFileId() >= 0 && head.getBlockPosn() >= 0) {
			this.head = head;
		}
		if (treeHead != null && treeHead.getFileId() >= 0 && treeHead.getBlockPosn() >= 0) {
			this.treeHead = treeHead;
		}
		this.unique = unique;
		this.meta = meta;
	}
	
	public static BTree create(boolean unique, BlockPtr head, TypeMetadata indexMeta, TransactionId txnId, Set<Object> pinnedBlocks) {
		IndexBranchBlock headBlock = (IndexBranchBlock) BlockManager.getInstance().createBranchBlock(head, pinnedBlocks);		
		IndexBlock treeRootBlock = (IndexBlock) BlockManager.getInstance().createIndexBlock(head.getFileId(), pinnedBlocks);
		BlockSerilizer.getInstance().serialize(treeRootBlock, indexMeta, txnId);
		BTreeImpl tree = new BTreeImpl(unique, head, treeRootBlock.getPtr(), treeRootBlock.getPtr(), indexMeta);
		tree.updateHeadRoot(headBlock, treeRootBlock.getPtr(), treeRootBlock.getPtr(), txnId);
		return tree;
	}
	
	public static BTreeImpl load(boolean unique, BlockPtr head, TypeMetadata meta, Set<Object> pinnedBlocks) {
		TypeMetadata headMeta = new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR);
		IndexBranchBlock headBlock = (IndexBranchBlock) BlockManager.getInstance().getBlock(head, headMeta, pinnedBlocks);
		IndexRecord record = (IndexRecord) headBlock.getData().get(0);
		DBType column = record.getColumn();
		BlockPtr treeHead = (BlockPtr) column;

		record = (IndexRecord) headBlock.getData().get(1);
		column = record.getColumn();		
		BlockPtr root = (BlockPtr) column;
		
		return new BTreeImpl(unique, head, treeHead, root, meta);
	}
	
	private void updateHeadRoot(Block headBlock, BlockPtr treeHead, BlockPtr root, TransactionId txnId) {
		
		this.treeHead = treeHead;
		this.root = root;
		
		IndexRecord indexRecord = null;
		if (headBlock.getData().size() == 0) {
			indexRecord = new IndexRecord();
			indexRecord.setColumn(treeHead);
			headBlock.getData().add(indexRecord);
			indexRecord = new IndexRecord();
			indexRecord.setColumn(root);
			headBlock.getData().add(indexRecord);
		} else {
			indexRecord = (IndexRecord) headBlock.getData().get(0);
			indexRecord.setColumn(treeHead);
			indexRecord = (IndexRecord) headBlock.getData().get(1);
			indexRecord.setColumn(root);
		}
		
		BlockSerilizer.getInstance().serialize(headBlock, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), txnId);		
	}
	
	@Override
	public ResultIterator getHead(boolean writeLock, Set<Object> pinnedBlocks) {
		readLock();
		try {
			IndexBlock headBlock = (IndexBlock) BlockManager.getInstance().getBlock(treeHead, meta, pinnedBlocks);
			if (writeLock) {
				headBlock.writeLock();
			} else {
				headBlock.readLock();
			}
			return new BTreeIteratorImpl(new BlockEntryPosition(headBlock, 0), this, writeLock, false, meta);
		} finally {
			readUnlock();
		}
	}
	
	@Override
	public IndexKeyType remove(IndexKeyType entry, Set<Object> pinnedBocks, TransactionId txnId) {
		boolean splitRequired = false;
		ObjectRecord record = null;
		while (true) {
			try {
				if (!splitRequired) {
					record = (ObjectRecord) removeInternal(entry, pinnedBocks, true, txnId);
				} else {
					record = (ObjectRecord) removeInternal(entry, pinnedBocks, false, txnId);
				}
				break;
			} catch (SplitRequiredException e) {
				splitRequired = true;
			}
		}
		if (record != null) {
			DBType column = record.getColumn();
			if (column instanceof ExtendedColumn) {
				return (IndexKeyType) ((ExtendedColumn) column).getValue(meta);
			}
			return (IndexKeyType) column;
		}
		return null;
	}

	private ObjectRecord removeInternal(IndexKeyType entry, Set<Object> pinnedBlocks, boolean readLock, TransactionId txnId) {
		Set<Object> findPinnedBlocks = new HashSet<Object>();
		IndexBlock rootBlock = null;
		IndexBlock changedBlock = null;
		Set<Block> cBlocks = new HashSet<Block>();
		BTreeIteratorImpl iter = null;
		ObjectRecord data = null;
		Stack<BlockPtr> callBlockStack = new Stack<BlockPtr>();
		try {
			if (readLock) {
				readLock();
			} else {
				writeLock();
			}
			rootBlock = (IndexBlock) BlockManager.getInstance().getBlock(root, meta, findPinnedBlocks);
			BlockEntryPosition bep = null;
			IndexCompareIndexQuery query = new IndexCompareIndexQuery(entry.getKey(), true, meta, findPinnedBlocks);
			bep = rootBlock.find(query, true, findPinnedBlocks, callBlockStack);
			iter = new BTreeIteratorImpl(bep, this, true, cBlocks, readLock, meta);
			if (iter.hasNext()) {
				data = (ObjectRecord) iter.next();
				if (data.getColumn().compareTo(entry.getKey()) == 0) {
					iter.remove();
				} else {
					Logger.getLogger(getClass()).fatal("Index: " + " " + data.getColumn().toString() + " " + entry.getKey().toString());
					data = null;
				}
			}
			
			Iterator<Block> cBlocksIter = cBlocks.iterator();
			while (cBlocksIter.hasNext()) {
				changedBlock = (IndexBlock) cBlocksIter.next();
				if (changedBlock != null) {
					CacheEntryPinner.getInstance().pin(changedBlock.getPtr(), pinnedBlocks);
				}
				break;
			}
			
			if (changedBlock == null) {
				Logger.getLogger(getClass()).fatal("remove did not remove an element");
				return null;
			}

			if (!root.equals(changedBlock.getPtr()) && changedBlock.getData().size() == 0) {
				// split
				Set<IndexBlock> changedBlocks = new HashSet<IndexBlock>();
				changedBlocks.add(changedBlock);
				removeRebalance(changedBlock, changedBlocks, findPinnedBlocks, txnId, callBlockStack);
				
				Iterator<IndexBlock> iter1 = changedBlocks.iterator();
				while (iter1.hasNext()) {
					IndexBlock ib = iter1.next();					
					CacheEntryPinner.getInstance().pin(ib.getPtr(), findPinnedBlocks);
					if (ib instanceof IndexLeafBlock) {
						BlockSerilizer.getInstance().serialize(ib, meta, txnId);
					} else {
						BlockSerilizer.getInstance().serialize(ib, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), txnId);						
					}
				}
			} else {				
				if (changedBlock instanceof IndexLeafBlock) {
					BlockSerilizer.getInstance().serialize(changedBlock, meta, txnId);
				} else {
					BlockSerilizer.getInstance().serialize(changedBlock, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), txnId);						
				}
			}

			if (data instanceof Extended) {
				RecordUtils.getInstance().releaseRecord(data);
			}
		} catch (SplitRequiredException e) {
			throw e;
		} finally {
			iter.unlock(true);
			if (readLock) {
				readUnlock();
			} else {
				writeUnlock();
			}
			CacheEntryPinner.getInstance().unpin(findPinnedBlocks, findPinnedBlocks);
		}
		return data;
	}
	
	private void removeRebalance(IndexBlock removeBlock, Set<IndexBlock> changedBlocks, Set<Object> pinnedBlocks, TransactionId txnId, Stack<BlockPtr> stack) {
		BlockPtr currentTreeHead = treeHead;
		BlockPtr currentRoot = root;
		removeRebalance(removeBlock, changedBlocks, pinnedBlocks, stack);
		if (currentRoot != root || currentTreeHead != treeHead) {
			Block headBlock = BlockManager.getInstance().getBlock(head, meta, pinnedBlocks);
			updateHeadRoot(headBlock, treeHead, root, txnId);
		}
	}
	
	private void removeRebalance(IndexBlock removeBlock, Set<IndexBlock> changedBlocks, Set<Object> pinnedBlocks, Stack<BlockPtr> stack) {
		IndexBlock blockToRemove = removeBlock;
		IndexBlock parent = null;
		IndexBlock prevBlock = null;
		IndexBlock nextBlock = null;
		if (removeBlock instanceof IndexLeafBlock) {
			prevBlock = (IndexLeafBlock) BlockManager.getInstance().getBlock(blockToRemove.getPrev(), meta, pinnedBlocks);
			nextBlock = (IndexLeafBlock) BlockManager.getInstance().getBlock(blockToRemove.getNext(), meta, pinnedBlocks);
			
			if (nextBlock != null) {
				changedBlocks.add(nextBlock);
				nextBlock.setPrev(removeBlock.getPrev());				
			}

			if (prevBlock != null) {
				changedBlocks.add(prevBlock);
				prevBlock.setNext(removeBlock.getNext());
			} else {
				treeHead = removeBlock.getNext();
			}
		} else {
			Logger.getLogger(getClass()).fatal("Invalid objet type: Expected IndexLeafBlock got:" + removeBlock.getClass());
		}
		
		while (blockToRemove != null) {
			parent = (IndexBlock) BlockManager.getInstance().getBlock(blockToRemove.getParent(stack), meta, pinnedBlocks);
			if (parent == null && blockToRemove instanceof IndexBranchBlock) {
				FreeBlockFactory.getInstance().returnBlock(blockToRemove.getPtr());
				IndexLeafBlock rootBlock = (IndexLeafBlock) BlockManager.getInstance().createIndexBlock(root.getFileId(), pinnedBlocks);
				root = rootBlock.getPtr();
				head = rootBlock.getPtr();
				rootBlock.setNext(null);
				rootBlock.setPrev(null);
				changedBlocks.add(rootBlock);
				blockToRemove = null;
				return;
			}
			int posn = -1;
			
			for (int i = 0; i < parent.getData().size(); i++) {
				ObjectRecord c1 = (ObjectRecord) parent.getData().get(i);
				if (c1 == null) {
					continue;
				}

				BlockPtr p1 = null;
				if (c1.getColumn() instanceof BlockPtr) {
					p1 = (BlockPtr) c1.getColumn();
				} else {
					throw new RuntimeException("Invalid type expected BlockPtr, got: " + p1 == null ? "null" : c1.getClass().toString());
				}
				
				if (p1.equals(blockToRemove.getPtr())) {
					changedBlocks.add(parent);
					posn = i;
					break;
				}
			}
			if (posn < 0) {
				Logger.getLogger(getClass()).fatal("remove postion is -ve during removeRebalance");
			}
			
			parent.getData().remove(posn);
			if (posn == parent.getData().size()) {
				updateMaxKey(parent, pinnedBlocks);
			}

			if (parent.getData().size() == 0) {
				blockToRemove = parent;
			} else {
				blockToRemove = null;
			}
			FreeBlockFactory.getInstance().returnBlock(removeBlock.getPtr());
		}
	}

	private void updateMaxKey(IndexBlock block, Set<Object> pinnedBlocks) {
		
		ObjectRecord record = (ObjectRecord) block.getData().get(block.getData().size()-1);
		DBType maxKey = null;
		DBType dt = record.getColumn();
		if (dt instanceof BlockPtr) {
			IndexBlock b = (IndexBlock) BlockManager.getInstance().getBlock((BlockPtr) dt, meta, pinnedBlocks);
			maxKey = b.getMaxKey(meta);
		} else {
			maxKey = record.getColumn();
		}
		block.setMaxKey(maxKey);		
	}
	
	private void insertRebalance(IndexBlock block, Set<Block> changedBlocks, Set<Object> pinnedBlocks, Stack<BlockPtr> callBlockStack) {
		List<IndexBlock> splitBlocks = null;
		IndexBlock blockToSplit = block;
		IndexBlock parent = null;
		
		while (blockToSplit != null) {			
			splitBlocks = SplitFactory.getInstance().split(blockToSplit, changedBlocks, pinnedBlocks, meta);
			
			changedBlocks.addAll(splitBlocks);
			parent = (IndexBlock) BlockManager.getInstance().getBlock(blockToSplit.getParent(callBlockStack), meta, pinnedBlocks); 
			IndexBlock newRoot = null;
			
			if (parent == null) {
				newRoot = (IndexBlock) BlockManager.getInstance().createBranchBlock(root.getFileId(), pinnedBlocks);
				changedBlocks.add(newRoot);
				CacheEntryPinner.getInstance().pin(newRoot.getPtr(), pinnedBlocks);
				for (int i = 0; i < splitBlocks.size(); i++) {
					IndexBlock b1 = splitBlocks.get(i);
					ObjectRecord record = new IndexRecord();
					record.setColumn(b1.getPtr());
					newRoot.getData().add(record);
					b1.setParent(newRoot.getPtr());
				}
				
				updateMaxKey(newRoot, pinnedBlocks);
				root = newRoot.getPtr();
				int size = BlockSerilizer.getInstance().getBlockSize(newRoot, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR));
				if (SplitFactory.getInstance().isSplitRequired(newRoot, size, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR))) {
					blockToSplit = newRoot;
				} else {
					blockToSplit = null;
				}
			} else {
				int posn = -1;
				for (int i = 0; i < parent.getData().size(); i++) {
					ObjectRecord record = (ObjectRecord) parent.getData().get(i);
					DBType dt = record.getColumn();
					if (dt.equals(blockToSplit.getPtr())) {
						posn = i;
						break;
					}
				}
				
				splitBlocks.remove(0);
				changedBlocks.add(parent);
				
				if (posn < 0) {
					Logger.getLogger(getClass()).fatal("insert postion is -ve during inserrtRebalance");
				}
				
				if (posn+1 >= parent.getData().size()) {
					for (Block bl : splitBlocks) {
						ObjectRecord record = new IndexRecord();
						record.setColumn(bl.getPtr());
						parent.getData().add(record);
					}
				} else {
					int p = posn+1;
					for (Block bl : splitBlocks) {
						ObjectRecord record = new IndexRecord();
						record.setColumn(bl.getPtr());
						parent.getData().add(p++, record);
					}
				} 
				updateMaxKey(parent, pinnedBlocks);
				
				int size = BlockSerilizer.getInstance().getBlockSize(parent, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR));
				if (!SplitFactory.getInstance().isSplitRequired(parent, size, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR))) {
					blockToSplit = null;
				} else {
					blockToSplit = parent;
				}
			}
		}
	}

	@Override
	public ResultIterator find(IndexQuery entry, boolean writeLock, Set<Object> pBlocks) {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		ResultIterator iter = null;
		Stack<BlockPtr> stack = new Stack<BlockPtr>();
		try {
			readLock();
			BlockEntryPosition bep = null;
			IndexBlock rootBlock = (IndexBlock) BlockManager.getInstance().getBlock(root, meta, pinnedBlocks);
			if (root != null) {
				bep = rootBlock.find(entry, writeLock, pinnedBlocks, stack);
			}
			
			iter = new BTreeIteratorImpl(bep, this, writeLock, false, meta);
			if (iter != null && iter.getCurrentBlock() != null && iter.getCurrentBlock().getPtr() != null) {
				CacheEntryPinner.getInstance().pin(iter.getCurrentBlock().getPtr(), pBlocks);
			}
		} finally {
			readUnlock();
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return iter;
	}
	
	@Override
	public ResultIterator iterator() {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		ResultIterator iter = null;
		readLock();
		try {
			IndexBlock block = (IndexBlock) BlockManager.getInstance().getBlock(treeHead, meta, pinnedBlocks);
			block.readLock();
			BlockEntryPosition bep = null;
			bep = new BlockEntryPosition(block, 0);
			iter = new BaseResultIteratorImpl(bep, false, meta) {
			};
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return iter; 
	}	
	
	@Override
	public  void insert(IndexKeyType data, Set<Object> pinnedBlocks, TransactionId txnId) {
		boolean splitRequired = false;
		while (true) {
			try {
				if (splitRequired) {
					insertInternal(data, pinnedBlocks, false, txnId);
				} else {
					insertInternal(data, pinnedBlocks, true, txnId);
				}
				break;
			} catch (SplitRequiredException e) {
				splitRequired = true;
			}
		}
	}
	
	private  void insertInternal(IndexKeyType data, Set<Object> pinnedBlocks, boolean readLock, TransactionId txnId) {
		BlockEntryPosition bep = null;
		Set<Block> changedBlocks = new HashSet<Block>();

		Set<Object> findPinnedBlocks = new HashSet<Object>();
		IndexCompareIndexQuery query = new IndexCompareIndexQuery(data, true, meta, pinnedBlocks);
		BTreeIteratorImpl iter = null;
		Iterator<Block> changedBlockIter = null;
		Block changedBlock = null;
		IndexRecord record = null;
		try {
			int recordSize = -1;
			if (readLock) {
				readLock();
			} else {
				writeLock();
			}
			Stack<BlockPtr> callBlockStack = new Stack<BlockPtr>();
			try {
				IndexBlock rootBlock = (IndexBlock) BlockManager.getInstance().getBlock(root, meta, pinnedBlocks);
				bep = rootBlock.find(query, true, findPinnedBlocks, callBlockStack);
				iter = new BTreeIteratorImpl(bep, this, true, changedBlocks, !readLock, meta);
				DBType column = null;
				if (iter.hasNext()) {
					record = (IndexRecord) iter.next();
					column = record.getColumn();
					IndexKeyType ikt = (IndexKeyType) column;
					if (unique && data.compareTo(ikt) == 0) {
						throw new UniqueKeyViolationException();
					}
				}
				record = new IndexRecord();
				record.setColumn(data);
				int maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(root);
				recordSize = RecordSerializer.getInstance().getRecordSize(record, meta);
				int maxSize = maxBlockSize - BlockSerilizer.INDEX_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
				if (recordSize >= maxSize) {
					record = (IndexRecord) RecordUtils.getInstance().convertToExtended(record, findPinnedBlocks, meta, maxSize, root.getFileId());						
				}
				IndexBlock ib = (IndexBlock) iter.getCurrentBlock();
				IndexKeyType blockMaxKey = (IndexKeyType) ib.getMaxKey(meta);
				if (ib.getNext() != null && blockMaxKey.compareTo(data) < 0) {
					Logger.getLogger(getClass()).fatal("insertint at the end");
				}
				iter.insert(record);					
				changedBlockIter = changedBlocks.iterator();					
	
				while (changedBlockIter.hasNext()) {
					changedBlock = changedBlockIter.next();
					if (changedBlock != null) {
						CacheEntryPinner.getInstance().pin(changedBlock.getPtr(), pinnedBlocks);
					}
					break;
				}
			} finally {
				CacheEntryPinner.getInstance().unpin(findPinnedBlocks, findPinnedBlocks);
			}
			
			try {
				if (changedBlock != null) {
					int size = BlockSerilizer.getInstance().getBlockSize(changedBlock, meta);
					if (SplitFactory.getInstance().isSplitRequired(changedBlock, size, meta)) {
						BlockPtr currentRoot = root;
						insertRebalance((IndexBlock) changedBlock, changedBlocks, pinnedBlocks, callBlockStack);
						if (!root.equals(currentRoot)) {
							Block headBlock = BlockManager.getInstance().getBlock(head, meta, findPinnedBlocks);
							updateHeadRoot(headBlock, treeHead, root, txnId);
						}
					}
					Iterator<Block> iter1 = changedBlocks.iterator();
					while(iter1.hasNext()) {
						Block b = iter1.next();
						if (b instanceof IndexBranchBlock) {
							BlockSerilizer.getInstance().serialize(b, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), txnId);
						} else {
							BlockSerilizer.getInstance().serialize(b, meta, txnId);
						}
						if (record.getColumn() instanceof Extended) {
							int maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(b.getPtr()) - BlockSerilizer.INDEX_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
							ColumnSerializer.getInstance().serializeExtended(b.getPtr().getFileId(), (ExtendedColumn) record.getColumn(), maxBlockSize, meta, findPinnedBlocks);
							List<BlockPtr> list = ((Extended)record.getColumn()).getPtrList();
							for (int i = 0; i < list.size(); i++) {
								BlockPtr p = list.get(i);
								CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
								SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(p);
								secondaryCacheHandler.changed(p);
								LogManager.getInstance().logBlock(txnId, serializedBlock);
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} finally {
			iter.unlock(true);
			if (readLock) {
				readUnlock();	
			} else {
				writeUnlock();
			}
			CacheEntryPinner.getInstance().unpin(findPinnedBlocks, findPinnedBlocks);
		}	
	}
		
	public void readLock() {
		rwLock.readLock().lock();
	}
	
	public void readUnlock() {
		rwLock.readLock().unlock();
	}
	
	public void writeLock() {
		rwLock.writeLock().lock();
	}
	
	public void writeUnlock() {
		rwLock.writeLock().unlock();
	}

	public BlockPtr getHeadPtr() {
		return head;
	}
	
	public BlockPtr getRootPtr() {
		return root;
	}
	
/*
	static CacheBean primaryCacheBean = new CacheBean();
	static CacheState primaryCacheState = new CacheState();
	static MemoryCacheMap<BlockPtr, List<Record>> primaryCacheMap = new MemoryCacheMap<>(10000, 5, true);
	static CacheLock cacheLock = new CacheLock();
	static CacheBean secondaryCacheBean = new CacheBean();
	static CacheState secondaryCacheState = new CacheState();
	static MemoryCacheMap<BlockPtr, ChannelBuffer> secondaryCacheMap = new MemoryCacheMap<>(10000, 5, true);
//	static SecondaryCacheResourceProvider resourceProvider = null;
	static int colId = -1;
	static CacheHandler<BlockPtr, List<Record>> primaryCacheHandler = null;
	static CacheHandler<BlockPtr, ChannelBuffer> secondaryCHandler = null;

	public static void main(String[] args) throws IOException {
		Set<Object> pinnedBlocks = new HashSet<>();

		BlockPtr p = new SingleBlockPtr((byte) 0, 0);
		pinnedBlocks.add(p);
		p = new SingleBlockPtr((byte) 0, 0);
		pinnedBlocks.clear();
		
		System.out.println(pinnedBlocks.contains(p));
    	WonderDBPropertyManager.getInstance().init("server.properties");
		primaryCacheBean.setCleanupHighWaterMark(WonderDBPropertyManager.getInstance().getPrimaryCacheHighWatermark()); // 1000
		primaryCacheBean.setCleanupLowWaterMark(WonderDBPropertyManager.getInstance().getPrimaryCacheLowWatermark()); // 999
		primaryCacheBean.setMaxSize(WonderDBPropertyManager.getInstance().getPrimaryCacheMaxSize()); // 1000
		PrimaryCacheResourceProvider primaryProvider = new PrimaryCacheResourceProvider(primaryCacheBean, primaryCacheState, cacheLock);
		PrimaryCacheResourceProviderFactory.getInstance().setResourceProvider(primaryProvider);
		primaryCacheHandler = new BaseCacheHandler<BlockPtr, List<Record>>(primaryCacheMap, primaryCacheBean, primaryCacheState, 
				cacheLock, primaryProvider, null);
		PrimaryCacheHandlerFactory.getInstance().setCacheHandler(primaryCacheHandler);
		
		secondaryCacheBean.setCleanupHighWaterMark(WonderDBPropertyManager.getInstance().getSecondaryCacheHighWatermark()); // 1475
		secondaryCacheBean.setCleanupLowWaterMark(WonderDBPropertyManager.getInstance().getSecondaryCacheLowWatermark()); // 1450
		secondaryCacheBean.setMaxSize(WonderDBPropertyManager.getInstance().getSecondaryCacheMaxSize()); // 1500
//		secondaryCacheBean.setStartSyncSize(10);
		CacheLock secondaryCacheLock = new CacheLock();
		SecondaryCacheResourceProvider secondaryProvider = new SecondaryCacheResourceProvider(null, secondaryCacheBean, secondaryCacheState, 
				secondaryCacheLock, 10000, 2048);
		SecondaryCacheResourceProviderFactory.getInstance().setResourceProvider(secondaryProvider);
		secondaryCHandler = new BaseCacheHandler<BlockPtr, ChannelBuffer>(secondaryCacheMap, 
				secondaryCacheBean, secondaryCacheState, secondaryCacheLock, secondaryProvider, null);
		SecondaryCacheHandlerFactory.getInstance().setCacheHandler(secondaryCHandler);
		
		CacheWriter<BlockPtr, ChannelBuffer> writer = new CacheWriter<>(secondaryCacheMap, 30000, new FileCacheWriter());
		writer.start();
		String fileName = WonderDBPropertyManager.getInstance().getSystemFile();
		FileBlockEntry entry = new FileBlockEntry();
		entry.setBlockSize(WonderDBPropertyManager.getInstance().getDefaultBlockSize());
		entry.setFileId((byte) 0);
		entry.setFileName(fileName);
		File raf = new File(fileName);
		WonderDBList storageList = null; // storage entry
		WonderDBList objectList = null; // name, head, 
		WonderDBList objectColumnList = null; // id, name, tablename, type, isnull
		WonderDBList indexList = null; // id name tableName, columnlist
		
		if (!raf.exists()) {
			FilePointerFactory.getInstance().create(entry);
			FreeBlockFactory.getInstance().createNewMgr(entry.getFileId(), null, 1, 2);
			FileBlockManager.getInstance().addFileBlockEntry(entry);
			
			BlockPtr ptr = new SingleBlockPtr((byte) 0 , 0);
			storageList = WonderDBList.create("metaStorageList", ptr, 1, meta, txnId, pinnedBlocks);
			
			ptr = new SingleBlockPtr((byte) 0 , 0 + 1*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			objectList = WonderDBList.create("objectList", ptr, 1, meta, txnId, pinnedBlocks);
					
			ptr = new SingleBlockPtr((byte) 0, 0+2*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			objectColumnList = WonderDBList.create("objectCoulumnList", ptr, 1, meta, txnId, pinnedBlocks);
			
			
			
		} else {
			FilePointerFactory.getInstance().create(entry);
			FileBlockManager.getInstance().addFileBlockEntry(entry);
			FreeBlockFactory.getInstance().createNewMgr(entry.getFileId(), 1, 2);
//			storageList = WonderDBList.load("storage", new SingleBlockPtr((byte) 0, 0), 
//					1, new ColumnSerializerMetadata(SerializerManager.FILE_BLOCK_ENRTY_TYPE), pinnedBlocks);			
		}
//		Column column = new Column();
//		column.setValue(entry);
//		ListRecord record = new ObjectListRecord(column);
//		storageList.add(record, null, pinnedBlocks);
		
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		pinnedBlocks.clear();
		
		
		List<Integer> list = new ArrayList<>();
		list.add(SerializerManager.INT);
		IndexRecordMetadata meta = new IndexRecordMetadata();
		meta.setTypeList(list);
		long posn = FreeBlockFactory.getInstance().getFreeBlockPosn(entry.getFileId());
//		BlockPtr head = new SingleBlockPtr(entry.getFileId(), posn);
//		BTree tree = BTreeImpl.create(true, head, meta, null, pinnedBlocks);
		BlockPtr head = new SingleBlockPtr(entry.getFileId(), 204800);
		BTree tree = BTreeImpl.load(true, head, meta, pinnedBlocks);
		List<DBType> l = new ArrayList<>();
		l.add(new IntType(10));
		RecordId recId = new RecordId(new SingleBlockPtr((byte)0, 0), 0);
		IndexKeyType ikt = new IndexKeyType(l, recId); 
		tree.insert(ikt, pinnedBlocks, null);
		l.clear();
		l.add(new IntType(5));
		ikt = new IndexKeyType(l, recId);
		tree.insert(ikt, pinnedBlocks, null);
		int i = 0;
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		i =10;
	}
*/
//	@Override
//	public String getSchemaObjectName() {
//		return schemaObjectName;
//	}
}