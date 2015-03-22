package org.wonderdb.block;

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

import java.util.Set;
import java.util.Stack;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.TypeMetadata;


public interface IndexBlock extends Block {
	DBType getMaxKey(TypeMetadata meta);
	void setMaxKey(DBType key);
	
	BlockEntryPosition find(IndexQuery entry, boolean writeLock, Set<Object> pinnedBlocks, Stack<BlockPtr> blockSTack);
}
