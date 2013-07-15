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
package org.wonderdb.block.index.impl.memory;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.index.impl.base.BaseIndexLeafBlock;

public class MemIndexLeafBlock extends BaseIndexLeafBlock {
	public String getSerializerName() {
		return null;
	}	
	
	public MemIndexLeafBlock(int schemaObjectId, SingleBlockPtr ptr) {
		super(ptr, schemaObjectId);
	}

	public BlockPtr getPtr() {
		throw new RuntimeException("Method not supported");
	}
}
