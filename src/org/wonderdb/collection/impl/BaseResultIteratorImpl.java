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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.types.Cacheable;


public abstract class BaseResultIteratorImpl implements ResultIterator {
	protected BlockEntryPosition currentPosn = null;
	private boolean writeLockBlock = false;
	protected Set<BlockPtr> pinnedBlocks = null;
	private Set<Block> changedBlocks = null;
	private List<Block> lockedBlocks = null;
	private boolean start = true;
	
	public BaseResultIteratorImpl(BlockEntryPosition bep, boolean writeLock) {
		this(bep, writeLock, new HashSet<Block>());
	}

//	protected BaseResultIteratorImpl(BlockEntryPosition bep, boolean writeLock) {
//		this(bep, writeLock, null, new HashSet<Block>());
//	}
//	
	public BaseResultIteratorImpl(BlockEntryPosition bep, boolean writeLock, Set<Block> changedBlocks) {
		// block is already locked 		
		lockedBlocks = new ArrayList<Block>();
		writeLockBlock = writeLock;
		this.pinnedBlocks = new HashSet<BlockPtr>();
		this.changedBlocks = changedBlocks;
		if (bep == null || bep.getBlock() == null) {
			return;
		}
		currentPosn = new BlockEntryPosition(bep.getBlock(), bep.getPosn());
		lockedBlocks.add(bep.getBlock());
//		pinnedBlocks.add(bep.getBlockPtr());
		CacheEntryPinner.getInstance().pin(bep.getBlockPtr(), pinnedBlocks);
	}
	
	public Cacheable nextEntry() {
		return (Cacheable) currentPosn.getBlock().getData().getUncached(currentPosn.getPosn());
	}
	
	@SuppressWarnings("unused")
	public boolean hasNext() {		
		if (currentPosn == null) {
			return false;
		}
		Block currentBlock = currentPosn.getBlock();
		if (currentBlock == null) {
			return false;
		}
		int posn = currentPosn.getPosn();
		if (!start) {
			posn = posn+1;
			currentPosn.setPosn(posn);
		} else {
			start = false;
		}
		
		if (posn < 0) {
			return false;
		}
		
		if (currentBlock != null && currentBlock.getData().size() <= posn && currentBlock.getNext() == null) {
			return false;
		}
		
		while (currentBlock != null && currentBlock.getData().size() <= posn && currentBlock.getNext() != null) {
			
//			CacheEntryPinner.getInstance().pin(currentBlock.getNext(), pinnedBlocks);
//			Block tmp = (Block) PrimaryCacheHandlerFactory.getInstance().getCacheHandler().get(currentBlock.getNext());
//			Block tmp = (Block) CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(currentBlock.getNext(), currentBlock.getSchemaObjectId(), pinnedBlocks);
//			PrimaryCacheHandlerFactory.getInstance().getCacheHandler().unpin(currentBlock.getPtr(), pinnedBlocks);
			Block tmp = getBlock(currentBlock.getNext());
			if (currentBlock.getNext() == null) {
				boolean bHere = true;
			}
			if (tmp != null) {
				lock(tmp);
				currentBlock = tmp;
				currentPosn.setBlock(currentBlock);
				currentPosn.setPosn(0);
				posn = 0;
			}
//			CacheEntryPinner.getInstance().unpin(currentBlock.getPtr(), pinnedBlocks);
		}
		return currentBlock != null && currentBlock.getData().size() > posn;
	}
	
	public void remove() {
		if (currentPosn != null && currentPosn.getBlock() != null) {
			changedBlocks.add(currentPosn.getBlock());
			currentPosn.getBlock().getData().remove(currentPosn.getPosn());
		}
	}
	
	public abstract Block getBlock(BlockPtr ptr);
	
	public void lock(Block block) {
		
		if (currentPosn.getBlock() == block || block == null) {
			return;
		}
		
		if (writeLockBlock) {
			block.writeLock();
		} else {
			block.readLock();
		}			
		
		List<Block> removedBlocks = new ArrayList<Block>();
		Set<BlockPtr> blocksToUnpin = new HashSet<BlockPtr>();
		
		for (int i = 0; i < lockedBlocks.size(); i++) {
			Block b = lockedBlocks.get(i);
			if (b != block) {
				if (writeLockBlock) {
					b.writeUnlock();
				} else {
					b.readUnlock();
				}
				removedBlocks.add(b);
				blocksToUnpin.add(b.getPtr());
			}
		}
		lockedBlocks.removeAll(removedBlocks);
		lockedBlocks.add(block);
		CacheEntryPinner.getInstance().unpin(blocksToUnpin, pinnedBlocks);
	}
	
	public void unlock() {
		unlock(true);
	}
	
	public void unlock(boolean shouldUnpin) {
		Set<BlockPtr> unpinPtrs = new HashSet<BlockPtr>();
		for (int i = 0; i < lockedBlocks.size(); i++) {
			if (writeLockBlock) {
				lockedBlocks.get(i).writeUnlock();
			} else {
				lockedBlocks.get(i).readUnlock();
			}
			unpinPtrs.add(lockedBlocks.get(i).getPtr());
		}
		if (shouldUnpin) {
			CacheEntryPinner.getInstance().unpin(unpinPtrs, pinnedBlocks);
		}
	}
	
	public int insert(Cacheable c) {
		if (currentPosn == null) {
			throw new RuntimeException("null current Position");
		}
		Block currentBlock = currentPosn.getBlock();
		if (currentBlock == null) {
			throw new RuntimeException("null current Block");
		}
		changedBlocks.add(currentBlock);
		return currentBlock.addEntry(currentPosn.getPosn(), c);
	}
	
	public boolean isAnyBlockEmpty() {
		if (currentPosn != null && currentPosn.getBlock() != null && currentPosn.getBlock().getData().size() == 0) {
			return true;
		}
		return false;
	}

	public Block getCurrentBlock() {
		if (currentPosn != null) {
			return currentPosn.getBlock();
		}
		return null;
	}
	
	public Set<BlockPtr> getPinnedSet() {
		return pinnedBlocks;
	}

}
