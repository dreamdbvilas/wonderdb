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
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexBlock;
import org.wonderdb.block.IndexBranchBlock;
import org.wonderdb.block.IndexLeafBlock;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.freeblock.FreeBlockFactory;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;


public class SplitFactory {
	private static SplitFactory instance = new SplitFactory();
	
	private SplitFactory() {
	}
	
	public static SplitFactory getInstance() {
		return instance;
	}
	
	public boolean isSplitRequired(Block block, int size, TypeMetadata meta) {
		int maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(block.getPtr()) - BlockSerilizer.INDEX_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
		if (size >= maxBlockSize) {
			return true;
		} 
		return false;
	}
	
	private List<List<Record>> split(IndexBlock block, TypeMetadata meta) {
		int overhead = BlockSerilizer.INDEX_BLOCK_HEADER + SerializedBlockImpl.HEADER_SIZE;
		int maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(block.getPtr()) - overhead;
		List<Record> list = block.getData();
		List<List<Record>> listOfList = new ArrayList<List<Record>>();
		List<Record> currentList = new ArrayList<>();
		int currentListSize = 0;
		listOfList.add(currentList);
		
		TypeMetadata newMeta = meta;
		if (block instanceof IndexBranchBlock) {
			newMeta = new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR);
		}
		for (int i = 0; i < list.size(); i++) {
			Record st = (Record) list.get(i);
			int size = RecordSerializer.getInstance().getRecordSize(st, newMeta);
			currentListSize = currentListSize + size;
			if (currentListSize >= maxBlockSize*0.7) {
				currentList = new ArrayList<Record>();
				currentList.add(st);
				listOfList.add(currentList);
				currentListSize = size;
			} else {
				currentList.add(st);
			}
		}
		return listOfList;
	}
	
	private void setMaxKey(IndexBlock block, TypeMetadata meta, Set<Object> pinnedBlocks) {
		IndexRecord lastRecord = (IndexRecord) block.getData().get(block.getData().size()-1); 
		DBType column = lastRecord.getColumn();
		DBType dt = column;
		if (dt instanceof BlockPtr) {
			IndexBlock b = (IndexBlock) BlockManager.getInstance().getBlock((BlockPtr) dt, meta, pinnedBlocks);
			block.setMaxKey(b.getMaxKey(meta));
		} else {
			block.setMaxKey(dt);
		}		
	}
	
	public List<IndexBlock> split(IndexBlock block, Set<Block> changedBlocks, Set<Object> pinnedBlocks, TypeMetadata meta) {
		List<IndexBlock> retList = new ArrayList<IndexBlock>();
		retList.add(block);
		boolean branchBlock = block instanceof IndexBranchBlock;
		List<List<Record>> splitList = split(block, meta);
		block.getData().clear();
		block.getData().addAll(splitList.get(0));

		setMaxKey(block, meta, pinnedBlocks);
		
//		BlockPtr parentPtr = block.getParent(stack);
		
		if (splitList.size() > 2) {
			Logger.getLogger(getClass()).fatal("split more");
		}
		for (int i = 1; i < splitList.size(); i++) {
			IndexBlock tmpBlock = null;
			long posn = FreeBlockFactory.getInstance().getFreeBlockPosn(block.getPtr().getFileId());
			BlockPtr ptr = new SingleBlockPtr(block.getPtr().getFileId(), posn);
			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
			if (branchBlock) {
				tmpBlock = (IndexBlock) BlockManager.getInstance().createBranchBlock(ptr, pinnedBlocks);	
			} else {
				tmpBlock = (IndexBlock) BlockManager.getInstance().createIndexLefBlock(ptr, pinnedBlocks);
			}
			CacheEntryPinner.getInstance().pin(tmpBlock.getPtr(), pinnedBlocks);
			tmpBlock.getData().addAll(splitList.get(i));
			retList.add(tmpBlock);
			changedBlocks.add(tmpBlock);
//			tmpBlock.setParent(parentPtr);
			setMaxKey(tmpBlock, meta, pinnedBlocks);
			if (!branchBlock) {
				IndexLeafBlock nextBlock = (IndexLeafBlock) BlockManager.getInstance().getBlock(block.getNext(), meta, pinnedBlocks);
				if (nextBlock != null) {
					nextBlock.setPrev(tmpBlock.getPtr());
					changedBlocks.add(nextBlock);
				}
				tmpBlock.setNext(block.getNext());
				block.setNext(tmpBlock.getPtr());
				tmpBlock.setPrev(block.getPtr());
			} else {
				for (int j = 0; j < tmpBlock.getData().size(); j++) {
					BlockPtr p = (BlockPtr) ((ObjectRecord) tmpBlock.getData().get(j)).getColumn();
					CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
					Block b = BlockManager.getInstance().getBlock(p, meta, pinnedBlocks);
					b.setParent(tmpBlock.getPtr());
//					changedBlocks.add(b);
				}
			}
		}
		
		return retList;
	}
}
