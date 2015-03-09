package org.wonderdb.block;

import java.util.Set;

import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.record.IndexRecord;

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


public class IndexBranchBlock extends BaseIndexBlock {
	public IndexBranchBlock(BlockPtr ptr) {
		super(ptr);
	}
	
	public BlockEntryPosition find(IndexQuery entry, boolean writeLock, Set<Object> pinnedBlocks) {
		int mid = findPosn(entry, writeLock, pinnedBlocks);

		if (mid >= getData().size()) {
			mid = getData().size()-1;
		}
		
		IndexRecord r = (IndexRecord) getData().get(mid);
		BlockPtr ptr = (BlockPtr) r.getColumn();
		IndexBlock block = (IndexBlock) BlockManager.getInstance().getBlock(ptr, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), pinnedBlocks);
		return block.find(entry, writeLock, pinnedBlocks);
		
	}

}
