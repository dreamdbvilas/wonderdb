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
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.collection.BTree;
import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.exceptions.SplitRequiredException;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.impl.IndexKeyType;



public class BTreeIteratorImpl extends BaseResultIteratorImpl {
	BTree tree = null;
	
	public BTreeIteratorImpl(BlockEntryPosition bep, BTree tree, boolean writeLock) {
		this(bep, tree, writeLock, new HashSet<Block>());
	}
	
	public BTreeIteratorImpl(BlockEntryPosition bep, BTree tree, boolean writeLock, Set<Block> changedBlocks) {
		super(bep, writeLock, changedBlocks);
		this.tree = tree;
	}
	
	public Block getCurrentRecordBlock() {
		IndexKeyType key = (IndexKeyType) nextEntry();
		if (key == null) {
			return null;
		}
		
		return CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(key.getRecordId().getPtr(), tree.getSchemaId(), pinnedBlocks);
	}
	
	public int getSchemaId() {
		return tree.getSchemaId();
	}
	
	public ResultContent next() {
		IndexKeyType key = (IndexKeyType) nextEntry();
		if (key == null) {
			return null;
		}
		return new IndexResultContent(key, getSchemaId());
	}
	
	public Block getBlock(BlockPtr ptr) {
		return CacheObjectMgr.getInstance().getIndexBlock(ptr, tree.getSchemaId(), pinnedBlocks);
	}
	
	public int insert(Cacheable c) {
		if (currentPosn == null) {
			throw new RuntimeException("null current Position");
		}
		
		Block currentBlock = currentPosn.getBlock();
		IndexBlock indexBlock = null;
		if (currentBlock instanceof IndexBlock) {
			indexBlock = (IndexBlock) currentBlock;
		}
		
		if (indexBlock == null) {
			throw new RuntimeException("null current Block");
		}
		
		if (indexBlock.isSplitRequired()) {
			throw new SplitRequiredException(indexBlock);
		}
		return super.insert(c);
	}
	
	public void remove() {
		if (currentPosn == null) {
			throw new RuntimeException("null current Position");
		}
		Block currentBlock = currentPosn.getBlock();
		IndexBlock indexBlock = null;
		if (currentBlock instanceof IndexBlock) {
			indexBlock = (IndexBlock) currentBlock;
		}
		
		if (indexBlock == null) {
			throw new RuntimeException("null current Block");
		}
		
		if (indexBlock.isSplitRequired()) {
			throw new SplitRequiredException(indexBlock);
		}
		super.remove();
	}	
}
