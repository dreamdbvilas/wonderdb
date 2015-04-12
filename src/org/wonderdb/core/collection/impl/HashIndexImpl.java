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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockEntryPosition;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexBlock;
import org.wonderdb.block.IndexCompareIndexQuery;
import org.wonderdb.block.IndexLeafBlock;
import org.wonderdb.block.IndexQuery;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.RecordUtils;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.DefaultSerializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.BlockPtrList;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.ObjectRecord;


public class HashIndexImpl implements BTree {
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static TypeMetadata BUCKET_META = new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR);
	private static int BUCKETS_PER_BLOCK = 150;
	
	TypeMetadata meta;
	BlockPtr head = null;
	List<BlockPtr> buckets = null;
	int allBuckets = 0;
	private AtomicLong links = new AtomicLong(0);
	
	private HashIndexImpl(BlockPtr head, TypeMetadata meta) {
		if (head != null && head.getFileId() >= 0 && head.getBlockPosn() >= 0) {
			this.head = head;
		}
		
		this.meta = meta;
	}
	
	public static BTree create(boolean unique, BlockPtr head, int bucketSize, TypeMetadata indexMeta, TransactionId txnId, Set<Object> pinnedBlocks) {
		HashIndexImpl tree = new HashIndexImpl(head, indexMeta);
		IndexLeafBlock headBlock = (IndexLeafBlock) BlockManager.getInstance().createIndexLefBlock(head, pinnedBlocks);
		List<BlockPtr> list = new ArrayList<BlockPtr>(bucketSize);
		list.add(head);
		IndexLeafBlock currentBlock = headBlock;
		Set<Object> pinnedBs = new HashSet<Object>();
		
		for (int i = 1; i < bucketSize; i++) {			
			IndexLeafBlock block = (IndexLeafBlock) BlockManager.getInstance().createIndexBlock(head.getFileId(), pinnedBs);
			currentBlock.setNext(block.getPtr());			
			tree.initList(currentBlock);
			
			BlockSerilizer.getInstance().serialize(currentBlock, BUCKET_META, txnId);
			currentBlock = block;
			list.add(block.getPtr());
			CacheEntryPinner.getInstance().unpin(block.getPtr(), pinnedBs);
		}
		tree.initList(currentBlock);
		BlockSerilizer.getInstance().serialize(currentBlock, BUCKET_META, txnId);
		tree.buckets = list;
		tree.allBuckets = list.size()*BUCKETS_PER_BLOCK;
		return tree;
	}
	
	public static HashIndexImpl load(BlockPtr head, TypeMetadata meta, Set<Object> pinnedBlocks) {
		Set<Object> localPinnedBlocks = new HashSet<Object>();
		IndexLeafBlock headBlock = (IndexLeafBlock) BlockManager.getInstance().getBlock(head, BUCKET_META, localPinnedBlocks);
		List<BlockPtr> list = new ArrayList<BlockPtr>();
		Block currentBlock = headBlock;
		while (currentBlock != null) {
			BlockPtr ptr = currentBlock.getPtr();
			list.add(ptr);
			currentBlock = BlockManager.getInstance().getBlock(currentBlock.getNext(), BUCKET_META, localPinnedBlocks);
			CacheEntryPinner.getInstance().unpin(localPinnedBlocks, localPinnedBlocks);
		}
		
		HashIndexImpl hii = new HashIndexImpl(head, meta);
		hii.buckets = list;
		hii.allBuckets = list.size()*BUCKETS_PER_BLOCK;
		return hii;
	}
	
	@Override
	public ResultIterator getHead(boolean writeLock, Set<Object> pinnedBlocks) {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public IndexKeyType remove(IndexKeyType entry, Set<Object> pinnedBocks, TransactionId txnId) {
		ObjectRecord record = null;
		record = (ObjectRecord) removeInternal(entry, pinnedBocks, false, txnId);
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
//		Set<Object> findPinnedBlocks = new HashSet<Object>();
		IndexBlock changedBlock = null;
		Set<Block> cBlocks = new HashSet<Block>();
		BTreeIteratorImpl iter = null;
		ObjectRecord data = null;
		Stack<BlockPtr> callBlockStack = new Stack<BlockPtr>();
		if (entry == null) {
			return null;
		}
		
		try {
			int hashCode = Math.abs(entry.hashCode());
			int posn = hashCode % buckets.size();
			BlockPtr ptr = buckets.get(posn);
			Block block = BlockManager.getInstance().getBlock(ptr, BUCKET_META, pinnedBlocks);
			IndexBlock indexBlock = null;
			BlockPtr ptrInBucket = null;
			
			block.readLock();
			try {
				IndexRecord record = (IndexRecord) block.getData().get(0);
				BlockPtrList list = (BlockPtrList) record.getColumn();
				int p = hashCode % BUCKETS_PER_BLOCK;
				if (list.getPtrList().size() <= p || list.getPtrList().get(p) == null || list.getPtrList().get(p).getBlockPosn() < 0) {
					return null;
				} else {
					ptrInBucket = list.getPtrList().get(p);
				}
			} finally {
				block.readUnlock();
			}
			indexBlock = (IndexBlock) BlockManager.getInstance().getBlock(ptrInBucket, meta, pinnedBlocks);
			
			boolean removed = false;
			BlockEntryPosition bep = null;
			IndexCompareIndexQuery query = new IndexCompareIndexQuery(entry.getKey(), true, meta, pinnedBlocks);
			while (indexBlock != null) {
				try {
					bep = indexBlock.find(query, true, pinnedBlocks, callBlockStack);
					iter = new BTreeIteratorImpl(bep, this, true, cBlocks, readLock, meta);
					if (iter.hasNext()) {
						data = (ObjectRecord) iter.next();
						if (data.getColumn().compareTo(entry.getKey()) == 0) {
							iter.remove();
							removed = true;
							Iterator<Block> cBlocksIter = cBlocks.iterator();
							while (cBlocksIter.hasNext()) {
								changedBlock = (IndexBlock) cBlocksIter.next();
								break;
							}
							
							BlockSerilizer.getInstance().serialize(changedBlock, meta, txnId);
							break;
						} else {
							indexBlock = (IndexBlock) BlockManager.getInstance().getBlock(indexBlock.getNext(), meta, pinnedBlocks);
						}
					}
				} finally {
					iter.unlock();
				}
			}
			
			if (removed) {
				if (data instanceof Extended) {
					RecordUtils.getInstance().releaseRecord(data);
				}
			}
		} finally {
		}
		return data;
	}
	
	private void initList(Block block) {
		for (int i = 0; i < BUCKETS_PER_BLOCK; i++) {
			IndexRecord record = new IndexRecord();
			record.setColumn(DefaultSerializer.NULL_BLKPTR);
			block.getData().add(record);
		}
	}

	@Override
	public ResultIterator find(IndexQuery entry, boolean writeLock, Set<Object> pBlocks) {
		Set<Object> pinnedBlocks = pBlocks;
		ResultIterator iter = null;
		IndexCompareIndexQuery query = null;
		if (entry == null ) {
			return EmptyResultIterator.getInstance();
		}
		
		if (entry instanceof IndexCompareIndexQuery) {
			query = (IndexCompareIndexQuery) entry;
		} else {
			throw new RuntimeException("Index Query type not supported" + entry);
		}
		
		BlockEntryPosition bep = null;
		Set<Block> cBlocks = new HashSet<Block>();
		ObjectRecord data = null;
		IndexBlock bucketBlock = null;
		
		try {
			int hashCode = Math.abs(query.hashCode());
			int posn = hashCode % allBuckets;
			
			int bucketPosn = posn % buckets.size(); 
			posn = posn / buckets.size();
			
			BlockPtr p = buckets.get(bucketPosn);
			bucketBlock = (IndexBlock) BlockManager.getInstance().getBlock(p, BUCKET_META, pinnedBlocks);
			bucketBlock.readLock();
			try {
				IndexRecord record = (IndexRecord) bucketBlock.getData().get(posn);
//				BlockPtrList list = (BlockPtrList) record.getColumn();
				p = (BlockPtr) record.getColumn();
//				int x = hashCode >> 15;
				if (p == null || p.getFileId() < 0) {
					return EmptyResultIterator.getInstance();
//					return null;
				}
			} finally {
				bucketBlock.readUnlock();
			}
			
			Stack<BlockPtr> callBlockStack = new Stack<BlockPtr>();
			boolean found = false;
			bucketBlock = (IndexBlock) BlockManager.getInstance().getBlock(p, meta, pinnedBlocks);
			while (bucketBlock != null) {
				try {
					bep = bucketBlock.find(query, writeLock, pinnedBlocks, callBlockStack);
					iter = new BTreeIteratorImpl(bep, this, writeLock, cBlocks, !writeLock, meta);
					data = (ObjectRecord) iter.peek();
					if (data != null && entry.compareTo(data.getColumn()) == 0) {
						found = true;
						return iter;
					} else {
						bucketBlock = (IndexBlock) BlockManager.getInstance().getBlock(bucketBlock.getNext(), meta, pinnedBlocks);						
					}
				} finally {
					if (!found && iter != null) {
						iter.unlock();
					}
				}
			}
		} finally {
//			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return EmptyResultIterator.getInstance();
	}
	
	@Override
	public ResultIterator iterator() {
		throw new RuntimeException("Method not supported");
	}	
	
	@Override
	public  void insert(IndexKeyType data, Set<Object> pinnedBlocks, TransactionId txnId) {
		insertInternal(data, pinnedBlocks, false, txnId);
	}
	
	private  void insertInternal(IndexKeyType data, Set<Object> pinnedBlocks, boolean readLock, TransactionId txnId) {
		BlockEntryPosition bep = null;
		Set<Block> changedBlocks = new HashSet<Block>();

		int hashCode = Math.abs(data.hashCode());
		int posn = hashCode % allBuckets;
		int bucketPosn =  posn % buckets.size();
		posn = posn / buckets.size();
		BlockPtr p = buckets.get(bucketPosn);
		boolean needWriteLock = false;
		IndexBlock indexBlock = null;

		IndexBlock bucketBlock = (IndexBlock) BlockManager.getInstance().getBlock(p, BUCKET_META, pinnedBlocks);

		bucketBlock.readLock();
		try {
//			BlockPtrList list = null;
			IndexRecord record = (IndexRecord) bucketBlock.getData().get(posn);
//			list = (BlockPtrList) record.getColumn();
//			int x = hashCode >> 15;
			BlockPtr p1 =((BlockPtr)record.getColumn()); 
			
			if (p1 == null || p1.getFileId() < 0) {
				needWriteLock = true;
			} else {
				indexBlock = (IndexBlock) BlockManager.getInstance().getBlock(p1, meta, pinnedBlocks);
			}
		} finally {
			bucketBlock.readUnlock();
		}
		
		if (needWriteLock) {
			bucketBlock.writeLock();
			try {
//				BlockPtrList list = null;
				IndexRecord record = (IndexRecord) bucketBlock.getData().get(posn);
//				list = (BlockPtrList) record.getColumn();
				BlockPtr p1 =((BlockPtr)record.getColumn()); 
				if (p1 == null || p1.getFileId() < 0) {
					indexBlock = (IndexLeafBlock) BlockManager.getInstance().createIndexBlock(head.getFileId(), pinnedBlocks);
					record.setColumn(indexBlock.getPtr());
					BlockSerilizer.getInstance().serialize(bucketBlock, BUCKET_META, txnId);
//					BlockSerilizer.getInstance().serialize(indexBlock, meta, txnId);
				} else {
					indexBlock = (IndexBlock) BlockManager.getInstance().getBlock(p1, meta, pinnedBlocks);
				}
			} finally {
				bucketBlock.writeUnlock();
			}
		}
		
		IndexCompareIndexQuery query = new IndexCompareIndexQuery(data, true, meta, pinnedBlocks);
		ResultIterator iter = null;
		IndexRecord record = null;
		IndexBlock prevBlock = null;
		IndexBlock firstBlock = null;
		try {
//			if (p == null || p.getBlockPosn() < 0) {
//				changedBlocks.add(indexBlock);
//			}
			int recordSize = -1;
			try {
//				iter = find(query, true, pinnedBlocks);
//				if (iter != null && iter != EmptyResultIterator.getInstance()) {
//					throw new UniqueKeyViolationException();
//				}
				record = new IndexRecord();
				record.setColumn(data);
				int maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(indexBlock.getPtr());
				recordSize = RecordSerializer.getInstance().getRecordSize(record, meta);
				int maxSize = maxBlockSize - BlockSerilizer.INDEX_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
				if (recordSize >= maxSize) {
					record = (IndexRecord) RecordUtils.getInstance().convertToExtended(record, pinnedBlocks, meta, maxSize, head.getFileId());						
					recordSize = RecordSerializer.getInstance().getRecordSize(record, meta);
				}
				
				firstBlock = indexBlock;
				prevBlock = indexBlock;
				firstBlock.writeLock();
				int count = 0;
				while (indexBlock != null) {		
					prevBlock = indexBlock;
					int blockSize = BlockSerilizer.getInstance().getBlockSize(indexBlock, meta);
					if ((blockSize + recordSize) >= maxSize) {
						indexBlock = (IndexBlock) BlockManager.getInstance().getBlock(indexBlock.getNext(), meta, pinnedBlocks);
						count++;
					} else {
						break;
					}
				}
				
				if (indexBlock == null) {
					indexBlock = (IndexBlock) BlockManager.getInstance().createIndexBlock(head.getFileId(), pinnedBlocks);
					count++;
					prevBlock.setNext(indexBlock.getPtr());
					changedBlocks.add(prevBlock);
				}
				boolean next = links.compareAndSet((count-1), count);
				if (next) {
					System.out.println(count);
				}
				Stack<BlockPtr> blockStack = new Stack<BlockPtr>();
				bep = indexBlock.find(query, true, pinnedBlocks, blockStack);
	
				if (bep.getPosn() >= indexBlock.getData().size()) {
					indexBlock.getData().add(record);
				} else {
					indexBlock.getData().add(bep.getPosn(), record);
				}
				changedBlocks.add(indexBlock);
				
				Iterator<Block> iter1 = changedBlocks.iterator();
				while(iter1.hasNext()) {
					Block b = iter1.next();
					BlockSerilizer.getInstance().serialize(b, meta, txnId);
				}
				
				if (record.getColumn() instanceof Extended) {
					maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(indexBlock.getPtr()) - BlockSerilizer.INDEX_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
					ColumnSerializer.getInstance().serializeExtended(indexBlock.getPtr().getFileId(), (ExtendedColumn) record.getColumn(), maxBlockSize, meta, pinnedBlocks);
					List<BlockPtr> list1 = ((Extended)record.getColumn()).getPtrList();
					for (int i = 0; i < list1.size(); i++) {
						BlockPtr p1 = list1.get(i);
						CacheEntryPinner.getInstance().pin(p1, pinnedBlocks);
						SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(p);
						secondaryCacheHandler.changed(p);
						LogManager.getInstance().logBlock(txnId, serializedBlock);
					}
				}
			} finally {
				indexBlock.writeUnlock();
			}
		} finally {
			firstBlock.writeUnlock();
			if (readLock) {
				readUnlock();	
			} else {
				writeUnlock();
			}
		}	
	}
		
	public void readLock() {
	}
	
	public void readUnlock() {
	}
	
	public void writeLock() {
	}
	
	public void writeUnlock() {
	}

	public BlockPtr getHeadPtr() {
		return head;
	}
	
	public BlockPtr getRootPtr() {
		return null;
	}	
}