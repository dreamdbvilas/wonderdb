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
package org.wonderdb.block.index.impl.base;

import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.index.IndexData;
import org.wonderdb.types.DBType;



public abstract class BaseIndexBranchBlock extends BaseIndexBlock  
	implements IndexBranchBlock, IndexData {
	
	public BaseIndexBranchBlock(BlockPtr ptr, int schemaObjectId) {
		super(ptr, schemaObjectId);
	}
	
	public DBType getKey() {
		return  getMaxKey();
	}
	
//	@Override
//	public int addEntry(int posn, IndexData data) {
//		int addPosn = super.addEntry(posn, data);
//		((IndexBlock) data).setParent(getPtr());
//		return addPosn;
//	}
	
//	public long getByteSize() {
//		return BranchBlockSerializer.getInstance().getByteSize(this);
//	}
//	
	public int findPosn(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks) {
		int mid = super.findPosn(entry, writeLock, pinnedBlocks);
		if (mid >= getData().size()) {
			return getData().size()-1;
		}
		return mid;
	}	
	
	public boolean isSplitRequired() {
		return false;
	}
	
	public void setSplitRequired(boolean flag) {
	}
//	
//	@Override
//	public int getSmallestBlockCountUsed() {
//		int smallBlockOverhead = Long.SIZE/8 + BlockPtrSerializer.BASE_SIZE;
//		int byteSize = getByteSize();
//		int blocksUsed = StorageUtils.getInstance().getBlocksRequired(getPtr(), byteSize, smallBlockOverhead, 0);
//		int smallestBlockCount = StorageUtils.getInstance().getSmallestBlockCount(getPtr());
//		return blocksUsed * smallestBlockCount;
//	}
}
