package org.wonderdb.block;

import java.util.Set;
import java.util.Stack;

import org.wonderdb.types.BlockPtr;

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

public class IndexLeafBlock extends BaseIndexBlock {
	public IndexLeafBlock(BlockPtr ptr) {
		super(ptr);
	}
	
	@Override
	public BlockEntryPosition find(IndexQuery entry, boolean writeLock, Set<Object> pinnedBlocks, Stack<BlockPtr> stack) {
		if (writeLock) {
			writeLock();
		} else {
			readLock();
		}
		int posn = findPosn(entry, writeLock, pinnedBlocks, stack);
		return new BlockEntryPosition(this, posn);
	}
}
