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
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockEntryPosition;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexBlock;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;



public class BTreeIteratorImpl extends BaseResultIteratorImpl {
	boolean treeWriteLock = false;
//	String schemaObjectName = null;
	
	public BTreeIteratorImpl(BlockEntryPosition bep, BTree tree, boolean blockWriteLock, boolean treeWriteLock, TypeMetadata meta) {
		this(bep, tree, blockWriteLock, new HashSet<Block>(), treeWriteLock, meta);
	}
	
	public BTreeIteratorImpl(BlockEntryPosition bep, BTree tree, boolean writeLock, Set<Block> changedBlocks, boolean treeWriteLock, TypeMetadata meta) {
		super(bep, writeLock, changedBlocks, meta);
		this.treeWriteLock = treeWriteLock;
//		this.schemaObjectName = tree.getSchemaObjectName();
	}
	
	public Block getCurrentRecordBlock() {
		Record key = (Record) next();
		if (key == null) {
			return null;
		}
		DBType column = ((ObjectRecord) key).getColumn();
		DBType ikt = column;
		if (ikt instanceof IndexKeyType) {
			return BlockManager.getInstance().getBlock( ((IndexKeyType) ikt).getRecordId().getPtr(), meta, pinnedBlocks);
		}
		throw new RuntimeException("Block not foind");
	}
	
	public void insert(Record c) {
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
		
		int size = RecordSerializer.getInstance().getRecordSize(c, meta);
		int blockSize = BlockSerilizer.getInstance().getBlockSize(indexBlock, meta);
		int maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(indexBlock.getPtr()) - BlockSerilizer.INDEX_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
		if (maxBlockSize - blockSize - size <= 0) {
			if (treeWriteLock) {
				super.insert(c);
			} else {
				throw new SplitRequiredException(indexBlock);
			}
		} else {
			super.insert(c);
		}
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

		if (currentBlock.getData().size() <= 1) {
			if (treeWriteLock) {
				super.remove();
			} else {
				throw new SplitRequiredException(indexBlock);
			}
		} else {
			super.remove();
		}
	}	
}
