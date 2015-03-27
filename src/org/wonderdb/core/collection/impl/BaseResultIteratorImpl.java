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
import java.util.List;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockEntryPosition;
import org.wonderdb.block.BlockManager;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.Record;


public abstract class BaseResultIteratorImpl implements ResultIterator {
	protected BlockEntryPosition currentPosn = null;
	private boolean writeLockBlock = false;
	protected Set<Object> pinnedBlocks = null;
	private Set<Block> changedBlocks = null;
	private List<Block> lockedBlocks = null;
	TypeMetadata meta = null;
	boolean firstTime = true;
	
	public BaseResultIteratorImpl(BlockEntryPosition bep, boolean writeLock, TypeMetadata meta) {
		this(bep, writeLock, new HashSet<Block>(), meta);
	}

	public BaseResultIteratorImpl(BlockEntryPosition bep, boolean blockWriteLock, Set<Block> changedBlocks, TypeMetadata meta) {
		// block is already locked 		
		this.meta = meta;
		lockedBlocks = new ArrayList<Block>();
		writeLockBlock = blockWriteLock;
		this.pinnedBlocks = new HashSet<Object>();
		this.changedBlocks = changedBlocks;
		if (bep == null || bep.getBlock() == null) {
			return;
		}
		currentPosn = new BlockEntryPosition(bep.getBlock(), bep.getPosn());
		lockedBlocks.add(bep.getBlock());
		CacheEntryPinner.getInstance().pin(bep.getBlockPtr(), pinnedBlocks);
	}
	
	public Record next() {
		Record record =  currentPosn.getBlock().getData().get(currentPosn.getPosn());
		return record;
	}
	
	public boolean hasNext() {		
		if (currentPosn == null) {
			return false;
		}

		if (!firstTime) {
			currentPosn.setPosn(currentPosn.getPosn()+1);
		} else {
			firstTime = false;
		}
		
		Block currentBlock = currentPosn.getBlock();
		if (currentBlock == null) {
			return false;
		}
		int posn = currentPosn.getPosn();
		
		if (posn < 0) {
			return false;
		}
		
		while (currentBlock != null && currentBlock.getData().size() <= posn && currentBlock.getNext() != null) {			
			Block tmp = BlockManager.getInstance().getBlock(currentBlock.getNext(), meta, pinnedBlocks);
			if (tmp != null) {
				lock(tmp);
				currentBlock = tmp;
				currentPosn.setBlock(currentBlock);
				currentPosn.setPosn(0);
				posn = 0;
			}
		}
		return currentBlock != null && currentBlock.getData().size() > posn;
	}
	
	public void remove() {
		if (currentPosn != null && currentPosn.getBlock() != null) {
			changedBlocks.add(currentPosn.getBlock());
			currentPosn.getBlock().getData().remove(currentPosn.getPosn());
		}
	}
	
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
		Set<Object> blocksToUnpin = new HashSet<Object>();
		
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
		Set<Object> unpinPtrs = new HashSet<Object>();
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
	
	public void insert(Record c) {
		if (currentPosn == null) {
			throw new RuntimeException("null current Position");
		}
		Block currentBlock = currentPosn.getBlock();
		if (currentBlock == null) {
			throw new RuntimeException("null current Block");
		}
		changedBlocks.add(currentBlock);
		
		if (currentPosn.getPosn() < 0) {
			currentBlock.getData().add(c);
		} else {
			currentBlock.getData().add(currentPosn.getPosn(), c);
		}
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
	
	public Set<Object> getPinnedSet() {
		return pinnedBlocks;
	}
	
	@Override
	public TypeMetadata getTypeMetadata() {
		return meta;
	}

}
