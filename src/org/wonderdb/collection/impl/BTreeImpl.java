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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.IndexData;
import org.wonderdb.block.index.IndexLeafBlock;
import org.wonderdb.block.index.factory.BlockFactory;
import org.wonderdb.block.index.factory.SplitFactory;
import org.wonderdb.block.index.impl.base.IndexCompareIndexQuery;
import org.wonderdb.block.index.impl.base.IndexQuery;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.BTree;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.exceptions.SplitRequiredException;
import org.wonderdb.exceptions.UpdateMaxKeyException;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.FreeBlocksMgr;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.block.SerializedContinuationBlock;
import org.wonderdb.seralizers.block.SerializedIndexBranchContinuationBlock;
import org.wonderdb.seralizers.block.SerializedIndexLeafContinuationBlock;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.IndexKeyType;
import org.wonderdb.types.metadata.impl.SerializedIndexMetadata;


public class BTreeImpl implements BTree {
	BlockPtr head = null;
	BlockPtr root = null;
	public ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	int idxId;
	boolean unique = false;
	static int splitCOunt = 0;
	Shard shard = null;
	
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();

	public BTreeImpl(int idxId, Shard shard, boolean unique, BlockPtr head, BlockPtr root) {
		this.idxId = idxId;
		if (root != null && root.getFileId() >= 0 && root.getBlockPosn() >= 0) {
			this.root = root;
		}
		if (head != null && head.getFileId() >= 0 && head.getBlockPosn() >= 0) {
			this.head = head;
		}
		this.unique = unique;
		this.shard = shard;
	}
	
	public int getSchemaId() {
		return idxId;
	}
	
	@Override
	public ResultIterator getHead(boolean writeLock, Set<BlockPtr> pinnedBlocks) {
		readLock();
		getRoot(pinnedBlocks, pinnedBlocks);
		IndexBlock headBlock = CacheObjectMgr.getInstance().getIndexBlock(head, idxId, pinnedBlocks);
		if (writeLock) {
			headBlock.writeLock();
		} else {
			headBlock.readLock();
		}
		return new BTreeIteratorImpl(new BlockEntryPosition(headBlock, 0), this, false);
	}
	
	@Override
	public void remove(IndexKeyType entry, Set<BlockPtr> pinnedBocks, TransactionId txnId) {
		boolean updateMaxKey = false;
		while (true) {
			try {
				if (!updateMaxKey) {
					removeInternal(entry, pinnedBocks, true, txnId);
				} else {
					removeInternal(entry, pinnedBocks, false, txnId);
				}
				break;
			} catch (SplitRequiredException e) {
			} catch (UpdateMaxKeyException e1) {
				updateMaxKey = true;
			}
		}
	}

	private void removeInternal(IndexKeyType entry, Set<BlockPtr> pinnedBlocks, boolean readLock, TransactionId txnId) {
		Set<BlockPtr> findPinnedBlocks = new HashSet<BlockPtr>();
		IndexBlock rootBlock = null;
		IndexBlock changedBlock = null;
		Set<Block> cBlocks = new HashSet<Block>();
		boolean mySplitRequired = false;
		BTreeIteratorImpl iter = null;
		IndexData data = null;
		boolean adjustMaxPtrs = false;
		try {
			if (readLock) {
				readLock();
			} else {
				writeLock();
			}
			rootBlock = getRoot(findPinnedBlocks, pinnedBlocks);
			BlockEntryPosition bep = null;
			IndexCompareIndexQuery query = new IndexCompareIndexQuery(entry.getKey());
			bep = rootBlock.find(query, true, findPinnedBlocks);
			iter = new BTreeIteratorImpl(bep, this, true, cBlocks);
			while (iter.hasNext()) {
				data = (IndexData) iter.nextEntry();
				if (data.getKey().compareTo(entry.getKey()) == 0) {
					if (iter.getCurrentBlock().getData().size()-1 <= iter.currentPosn.getPosn()) {
						if (readLock) {
							throw new UpdateMaxKeyException();
						}
						adjustMaxPtrs = true;
					}
					iter.remove();
					if (adjustMaxPtrs) {
						if (iter.getCurrentBlock().getData().size() > 0) {
							adjustMaxPtrs((IndexBlock) iter.getCurrentBlock(), findPinnedBlocks);
						}
					}
					break;
				} else {
					Logger.getLogger(getClass()).fatal("Index: " + idxId + " " + data.getKey().toString() + " " + entry.getKey().toString());
					break;
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
				return;
			}
			
			if (changedBlock.getData().size() == 0 && !changedBlock.getPtr().equals(rootBlock.getPtr())) {
				mySplitRequired = true;
				changedBlock.setSplitRequired(true);
			} else {
				SerializedContinuationBlock contBlock = CacheObjectMgr.getInstance().updateIndexBlock(changedBlock, idxId, pinnedBlocks);
				for (int i = 0; i < contBlock.getBlocks().size(); i++) {
					LogManager.getInstance().logBlock(txnId, contBlock.getBlocks().get(i));
					secondaryCacheHandler.changed(contBlock.getBlocks().get(i));
				}
			}
		} finally {
			iter.unlock(true);
			if (readLock) {
				readUnlock();
			} else if (!mySplitRequired) {
				writeUnlock();
			}
			if (!mySplitRequired) {
				CacheEntryPinner.getInstance().unpin(findPinnedBlocks, findPinnedBlocks);
			}
		}
		
		if (!mySplitRequired) {
			return;
		}
		
		Set<IndexBlock> finalChangedBlocks = new HashSet<IndexBlock>();
		try {
			if (readLock) {
				writeLock();
			}
			finalChangedBlocks.add(changedBlock);
			IndexBlock splitBlock = null;
			if (changedBlock.getData().size() == 0) {
				splitBlock = changedBlock;
				changedBlock.setSplitRequired(false);
			} else {
				splitBlock = null;
			}
			
			if (splitBlock != null) {
				removeRebalance(splitBlock, finalChangedBlocks, pinnedBlocks);
			}
			Iterator<IndexBlock> iter1 = finalChangedBlocks.iterator();
			while (iter1.hasNext()) {
				IndexBlock b = iter1.next();
				SerializedContinuationBlock contBlock = CacheObjectMgr.getInstance().updateIndexBlock(b, idxId, pinnedBlocks);
				for (int i = 0; i < contBlock.getBlocks().size(); i++) {
					LogManager.getInstance().logBlock(txnId, contBlock.getBlocks().get(i));
					secondaryCacheHandler.changed(contBlock.getBlocks().get(i));
				}
			}
		} finally {
			writeUnlock();
			changedBlock.setSplitRequired(false);
			CacheEntryPinner.getInstance().unpin(findPinnedBlocks, findPinnedBlocks);
		}
	}
	
	private void removeRebalance(IndexBlock removeBlock, Set<IndexBlock> changedBlocks, Set<BlockPtr> pinnedBlocks) {
		IndexBlock blockToRemove = removeBlock;
		IndexBlock parent = null;
		boolean adjustMaxPtrs = false;
		IndexLeafBlock prevBlock = null;
		IndexLeafBlock nextBlock = null;
		if (removeBlock instanceof IndexLeafBlock) {
			prevBlock = (IndexLeafBlock) CacheObjectMgr.getInstance().getIndexBlock(blockToRemove.getPrev(), idxId, pinnedBlocks);
			nextBlock = (IndexLeafBlock) CacheObjectMgr.getInstance().getIndexBlock(blockToRemove.getNext(), idxId, pinnedBlocks);
			
			if (nextBlock != null) {
				changedBlocks.add(nextBlock);
				nextBlock.setPrev(removeBlock.getPrev());				
			}

			if (prevBlock != null) {
				changedBlocks.add(prevBlock);
				prevBlock.setNext(removeBlock.getNext());
			} else {
				head = removeBlock.getNext();
				updateRootHeadPtr();
			}
		} else {
			Logger.getLogger(getClass()).fatal("Invalid objet type: Expected IndexLeafBlock got:" + removeBlock.getClass());
		}
		
		while (blockToRemove != null) {
			try {
				Block removedBlock = blockToRemove;
				parent = CacheObjectMgr.getInstance().getIndexBlock(blockToRemove.getParent(), idxId, pinnedBlocks); 
				if (parent == null) {
					IndexLeafBlock rootBlock = BlockFactory.getInstance().createIndexLeafBlock(idxId, shard, pinnedBlocks);
					root = rootBlock.getPtr();
					head = rootBlock.getPtr();
					rootBlock.setNext(null);
					rootBlock.setPrev(null);
					updateRootHeadPtr();
					changedBlocks.add(rootBlock);
					blockToRemove = null;
					return;
				}
				int posn = -1;
				
				for (int i = 0; i < parent.getData().size(); i++) {
					Cacheable c1 = parent.getData().getUncached(i);
					if (c1 == null) {
						continue;
					}

					BlockPtr p1 = null;
					if (c1 instanceof BlockPtr) {
						p1 = (BlockPtr) c1;
					} else {
						throw new RuntimeException("Invalid type expected BlockPtr, got: " + p1 == null ? "null" : c1.getClass().toString());
					}
					
					if (p1.equals(blockToRemove.getPtr())) {
						changedBlocks.add(parent);
						posn = i;
						if (parent.getData().size()-1 == posn) {
							adjustMaxPtrs = true;
						}
						break;
					}
				}
				if (posn < 0) {
					Logger.getLogger(getClass()).fatal("remove postion is -ve during removeRebalance");
				}
				
				parent.getData().remove(posn);

				if (parent.getData().size() == 0) {
					blockToRemove = parent;
				} else {
					if (adjustMaxPtrs) {
						adjustMaxPtrs(parent, pinnedBlocks);
					}
					blockToRemove = null;
				}
				FreeBlocksMgr.getInstance().add(idxId, removedBlock.getPtr());
			} finally {
			}
		}
	}

	private void adjustMaxPtrs(IndexBlock block, Set<BlockPtr> pinnedBlocks) {
		Cacheable cacheable = block.getData().getUncached(block.getData().size()-1);
		BlockPtr lastPtr = null;
		if (cacheable instanceof BlockPtr) {
			lastPtr = (BlockPtr) cacheable;
			CacheEntryPinner.getInstance().pin(lastPtr, pinnedBlocks);
			IndexBlock b = CacheObjectMgr.getInstance().getIndexBlock(lastPtr, idxId, pinnedBlocks);
			block.setMaxKey(b.getMaxKey());
		} else if (cacheable instanceof DBType){
			block.setMaxKey((DBType) cacheable);
		} else {
			System.out.println("issue");
		}
		
		IndexBlock parent = CacheObjectMgr.getInstance().getIndexBlock(block.getParent(), idxId, pinnedBlocks);

		if (parent != null && isLast(parent, block)) {
			adjustMaxPtrs(parent, pinnedBlocks);
		}
	}
	
	private boolean isLast(IndexBlock parent, IndexBlock block) {
		BlockPtr lastPtr = (BlockPtr) parent.getData().getUncached(parent.getData().size()-1);
		if (lastPtr.equals(block.getPtr())) {
			return true;
		}
		return false;
	}
	
	private void insertRebalance(IndexBlock block, Set<Block> changedBlocks, Set<BlockPtr> pinnedBlocks) {
		List<IndexBlock> splitBlocks = null;
		IndexBlock blockToSplit = block;
		IndexBlock parent = null;
		boolean adjustMaxPtrs = false;
		
		while (blockToSplit != null) {
			
			splitBlocks = SplitFactory.getInstance().split(blockToSplit, idxId, shard, changedBlocks, pinnedBlocks);
			
			changedBlocks.addAll(splitBlocks);
			parent = CacheObjectMgr.getInstance().getIndexBlock(blockToSplit.getParent(), idxId, pinnedBlocks); 
			IndexBlock newRoot = null;
			
			if (parent == null) {
				newRoot = BlockFactory.getInstance().createIndexBranchBlock(idxId, shard, pinnedBlocks);
				changedBlocks.add(newRoot);
				CacheEntryPinner.getInstance().pin(newRoot.getPtr(), pinnedBlocks);
				for (int i = 0; i < splitBlocks.size(); i++) {
					IndexBlock b1 = splitBlocks.get(i);
					newRoot.getData().add(b1.getPtr());
					b1.setParent(newRoot.getPtr());
				}
				
				newRoot.setMaxKey(((IndexData) newRoot.getData().get(newRoot.getData().size()-1)).getKey());
				root = newRoot.getPtr();
				if (SplitFactory.getInstance().isSplitRequired(newRoot, getBlockSize(newRoot))) {
					blockToSplit = newRoot;
				} else {
					blockToSplit = null;
				}
			} else {
				int posn = -1;
				for (int i = 0; i < parent.getData().size(); i++) {
					if (parent.getData().getUncached(i).equals(blockToSplit.getPtr())) {
						posn = i;
						break;
					}
				}
				
				if (parent.getData().size()-1 == posn) {
					adjustMaxPtrs = true;
				}
				splitBlocks.remove(0);
				changedBlocks.add(parent);
				
				if (posn < 0) {
					Logger.getLogger(getClass()).fatal("insert postion is -ve during removeRebalance");
				}
				
				if (posn+1 >= parent.getData().size()) {
					for (Block bl : splitBlocks) {
						parent.getData().add(bl.getPtr());
					}
				} else {
					int p = posn+1;
					for (Block bl : splitBlocks) {
						parent.getData().add(p++, bl.getPtr());
					}
				} 
				
				parent.setMaxKey(((IndexData) parent.getData().getAndPin(parent.getData().size()-1, pinnedBlocks)).getKey());
				if (adjustMaxPtrs) {
					adjustMaxPtrs(parent, pinnedBlocks);
				}
				
				if (!SplitFactory.getInstance().isSplitRequired(parent, getBlockSize(parent))) {
					blockToSplit = null;
				} else {
					blockToSplit = parent;
				}
			}
		}
	}

	@Override
	public ResultIterator find(IndexQuery entry, boolean writeLock, Set<BlockPtr> pBlocks) {
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		ResultIterator iter = null;
		try {
			readLock();
			BlockEntryPosition bep = null;
			IndexBlock rootBlock = CacheObjectMgr.getInstance().getIndexBlock(root, idxId, pinnedBlocks);
			if (root != null) {
				bep = rootBlock.find(entry, writeLock, pinnedBlocks);
			}
			
			iter = new BTreeIteratorImpl(bep, this, writeLock);
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
	public  void insert(IndexKeyType data, Set<BlockPtr> pinnedBlocks, TransactionId txnId) {
		boolean updateMaxKey = false;
		while (true) {
			try {
				if (!updateMaxKey) {
					insertInternal(data, pinnedBlocks, true, txnId);
				} else {
					insertInternal(data, pinnedBlocks, false, txnId);
				}
				break;
			} catch (Exception e) {
				if (e instanceof UpdateMaxKeyException) {
					updateMaxKey = true;
				}
			}
		}
	}
	
	private  void insertInternal(IndexKeyType data, Set<BlockPtr> pinnedBlocks, boolean readLock, TransactionId txnId) {
		BlockEntryPosition bep = null;
		Set<Block> changedBlocks = new HashSet<Block>();

		Set<BlockPtr> findPinnedBlocks = new HashSet<BlockPtr>();
		IndexCompareIndexQuery query = new IndexCompareIndexQuery(data);
		BTreeIteratorImpl iter = null;
		Iterator<Block> changedBlockIter = null;
		Block changedBlock = null;
		boolean mySplitRequired = false;
		boolean adjustMaxPtrs = false;
		try {
			if (readLock) {
				readLock();
			} else {
				writeLock();
			}
			try {
				IndexBlock rootBlock = getRoot(findPinnedBlocks, pinnedBlocks);
				bep = rootBlock.find(query, true, findPinnedBlocks);
				iter = new BTreeIteratorImpl(bep, this, true, changedBlocks);
				IndexBlock currentBlock = (IndexBlock) iter.getCurrentBlock();
				IndexBlock parentBlock = CacheObjectMgr.getInstance().getIndexBlock(currentBlock.getParent(), idxId, findPinnedBlocks);
				if (parentBlock != null && parentBlock.getMaxKey().compareTo(data) < 0) {
					iter.hasNext();
				}
				currentBlock = (IndexBlock) iter.getCurrentBlock();
				if (currentBlock.getData().size()-1 <= iter.currentPosn.getPosn()) {
					adjustMaxPtrs = true;
					if (readLock) {
						throw new UpdateMaxKeyException();
					}
				}
				iter.insert(data);					
				if (adjustMaxPtrs) {
					adjustMaxPtrs(currentBlock, findPinnedBlocks);
				}
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
			
			if (changedBlock != null) {
				if (SplitFactory.getInstance().isSplitRequired(changedBlock, getBlockSize((IndexBlock) changedBlock))) {
					mySplitRequired = true;
					((IndexBlock) changedBlock).setSplitRequired(true);
				}
			}
			try {
				if (!mySplitRequired && changedBlock != null) {
					secondaryCacheHandler.changed(changedBlock.getPtr());
					SerializedContinuationBlock contBlock = CacheObjectMgr.getInstance().updateIndexBlock((IndexBlock) changedBlock, idxId, pinnedBlocks);
					for (int i = 0; i < contBlock.getBlocks().size(); i++) {
						LogManager.getInstance().logBlock(txnId, contBlock.getBlocks().get(i));
						secondaryCacheHandler.changed(contBlock.getBlocks().get(i));
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
				if (!mySplitRequired) {
					writeUnlock();
				}
			}
			CacheEntryPinner.getInstance().unpin(findPinnedBlocks, findPinnedBlocks);
		}	
		
		if (!mySplitRequired) {
			return;
		}
		
		findPinnedBlocks.clear();
		try {
			if (readLock) {
				writeLock();
			}
			if (SplitFactory.getInstance().isSplitRequired(changedBlock, getBlockSize((IndexBlock) changedBlock))) {
				BlockPtr currentRoot = root;
				insertRebalance((IndexBlock) changedBlock, changedBlocks, pinnedBlocks);
				if (!root.equals(currentRoot)) {
					updateRootHeadPtr();
				}
			}
			((IndexBlock) changedBlock).setSplitRequired(false);
			Iterator<Block> iter1 = changedBlocks.iterator();
			while (iter1.hasNext()) {
				IndexBlock b = (IndexBlock) iter1.next();
				secondaryCacheHandler.changed(b.getPtr());
				SerializedContinuationBlock contBlock = CacheObjectMgr.getInstance().updateIndexBlock(b, idxId, pinnedBlocks);
				for (int i = 0; i < contBlock.getBlocks().size(); i++) {
					LogManager.getInstance().logBlock(txnId, contBlock.getBlocks().get(i));
					secondaryCacheHandler.changed(contBlock.getBlocks().get(i));
				}				
			}
		} finally {
			writeUnlock();
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

	private IndexBlock getRoot(Set<BlockPtr> readPinnedBlocks, Set<BlockPtr> writePinnedBlocks) {
		if (root != null) {
			return (IndexBlock) CacheObjectMgr.getInstance().getIndexBlock(root, idxId, readPinnedBlocks);
		}
		synchronized (this) {
			if (root == null) {
				IndexBlock block = BlockFactory.getInstance().createIndexLeafBlock(idxId, shard, writePinnedBlocks);
				root = block.getPtr();
				head = block.getPtr();
				updateRootHeadPtr();
				return block;
			}			
		}
		return (IndexBlock) CacheObjectMgr.getInstance().getIndexBlock(root, idxId, readPinnedBlocks);
	}
	
	private void updateRootHeadPtr() {
//		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex(idxId);
		TransactionId txnId = LogManager.getInstance().startTxn();
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		try {
			SerializedIndexMetadata sim = new SerializedIndexMetadata();
			byte fileId = FileBlockManager.getInstance().getId(shard);
			sim.updateHeadRoot(fileId, head, root, txnId, pinnedBlocks);
			LogManager.getInstance().commitTxn(txnId);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}

	public int getBlockSize(IndexBlock block) {
		int maxBlockSize = StorageUtils.getInstance().getMaxChunkSize(block.getPtr());
		if (block instanceof IndexLeafBlock) {
			return maxBlockSize - SerializedIndexLeafContinuationBlock.HEADER_SIZE;
		}
		return maxBlockSize - SerializedIndexBranchContinuationBlock.HEADER_SIZE;
	}

	@Override
	public Shard getShard() {
		return shard;
	}
}
