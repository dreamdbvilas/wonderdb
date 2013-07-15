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
package org.wonderdb.block.index.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.index.IndexData;
import org.wonderdb.block.index.IndexLeafBlock;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cluster.Shard;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.BlockPtrListSerializer;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.types.SerializableType;


public class SplitFactory {
	private static SplitFactory instance = new SplitFactory();
	
	private SplitFactory() {
	}
	
	public static SplitFactory getInstance() {
		return instance;
	}
	
	public boolean isSplitRequired(Block block, int maxBlockSize) {
		long blockSize = block.getByteSize();
		
		if (blockSize > maxBlockSize && block.getData().size() > 0) {
			return true;
		} 
		return false;
	}
	
	public boolean isSplitRequired(Block block, int maxBlockSize, SerializableType data) {
		int blockSize = block.getByteSize();
		
		if (blockSize < maxBlockSize) {
			return false;
		}
		return true;
	}
	
	private List<List<SerializableType>> split(IndexBlock block) {
		int overhead = SerializedBlockImpl.HEADER_SIZE + (5 * BlockPtrListSerializer.BASE_SIZE);
		int maxChunkSize = StorageUtils.getInstance().getTotalBlockSize(block.getPtr()) - overhead;
		CacheableList list = block.getData();
		List<List<SerializableType>> listOfList = new ArrayList<List<SerializableType>>();
		List<SerializableType> currentList = null;
//		listOfList.add(currentList);
		int currentListSize = 0;
		
		for (int i = 0; i < list.size(); i++) {
			SerializableType st = (SerializableType) list.getUncached(i);
			int size = st.getByteSize();
			if (size >= maxChunkSize) {
				currentList = new ArrayList<SerializableType>();
				currentList.add(st);
				listOfList.add(currentList);
				currentListSize = 0;
				currentList = null;
			} else if (size + currentListSize >= maxChunkSize*0.8) {
				currentList = new ArrayList<SerializableType>();
				currentList.add(st);
				listOfList.add(currentList);
				currentListSize = size;
//				currentList = null;
			} else {
				if (currentList == null) {
					currentList = new ArrayList<SerializableType>();
					listOfList.add(currentList);
				}
				currentList.add(st);
				currentListSize = currentListSize + size;
			}
		}
		return listOfList;
	}
	
	public List<IndexBlock> split(IndexBlock block, int idxId, Shard shard, Set<Block> changedBlocks, Set<BlockPtr> pinnedBlocks) {
		List<IndexBlock> retList = new ArrayList<IndexBlock>();
		retList.add(block);
		boolean branchBlock = false;
		if (block instanceof IndexBranchBlock) {
			branchBlock = true;
		}
		List<List<SerializableType>> splitList = split(block);
		block.getData().clear();
		block.getData().addAll(splitList.get(0));

		block.setMaxKey(((IndexData) block.getData().getAndPin(block.getData().size()-1, pinnedBlocks)).getKey());
		
		BlockPtr parentPtr = block.getParent();
		if (splitList.size() > 2) {
			Logger.getLogger(getClass()).fatal("split more");
		}
		for (int i = 1; i < splitList.size(); i++) {
			IndexBlock tmpBlock = null;
			if (branchBlock) {
				tmpBlock = BlockFactory.getInstance().createIndexBranchBlock(idxId, shard, pinnedBlocks);	
			} else {
				tmpBlock = BlockFactory.getInstance().createIndexLeafBlock(idxId, shard, pinnedBlocks);
			}
			CacheEntryPinner.getInstance().pin(tmpBlock.getPtr(), pinnedBlocks);
			tmpBlock.getData().addAll(splitList.get(i));
			retList.add(tmpBlock);
			changedBlocks.add(tmpBlock);
			tmpBlock.setParent(parentPtr);
			tmpBlock.setMaxKey(((IndexData) tmpBlock.getData().getAndPin(tmpBlock.getData().size()-1, pinnedBlocks)).getKey());
			if (!branchBlock) {
				IndexLeafBlock nextBlock = (IndexLeafBlock) CacheObjectMgr.getInstance().getIndexBlock(block.getNext(), idxId, pinnedBlocks);
				if (nextBlock != null) {
					nextBlock.setPrev(tmpBlock.getPtr());
					changedBlocks.add(nextBlock);
				}
				tmpBlock.setNext(block.getNext());
				block.setNext(tmpBlock.getPtr());
				tmpBlock.setPrev(block.getPtr());
			} else {
				for (int j = 0; j < tmpBlock.getData().size(); j++) {
					BlockPtr p = (BlockPtr) tmpBlock.getData().getUncached(j);
					CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
					Block b = CacheObjectMgr.getInstance().getIndexBlock(p, block.getSchemaObjectId(), pinnedBlocks);
					b.setParent(tmpBlock.getPtr());
					changedBlocks.add(b);
				}
			}
		}
		
		return retList;
	}
}
