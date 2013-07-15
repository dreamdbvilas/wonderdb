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
package org.wonderdb.block.index;

import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.impl.base.IndexQuery;
import org.wonderdb.collection.impl.BlockEntryPosition;
import org.wonderdb.types.DBType;


public interface IndexBlock extends Block, IndexData {
	DBType getMaxKey();
	void setMaxKey(DBType key);
	
	BlockEntryPosition find(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks);
	int findPosn(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks);
	boolean isSplitRequired();
	void setSplitRequired(boolean flag);
	public void setBufferCount(int count);
}
