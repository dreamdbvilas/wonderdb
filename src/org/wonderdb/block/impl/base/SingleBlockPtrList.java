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
package org.wonderdb.block.impl.base;

import java.util.ArrayList;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;


public class SingleBlockPtrList extends ArrayList<BlockPtr> implements SerializableType, DBType {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7769475470592441772L;
	
	public SingleBlockPtrList() {
	}
	
	@Override
	public String getSerializerName() {
		return "bl";
	}

	@Override
	public int getByteSize() {
		return Integer.SIZE/8 + (size() * BlockPtrSerializer.BASE_SIZE);
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public SingleBlockPtrList copyOf() {
		SingleBlockPtrList list = new SingleBlockPtrList();
		for (int i = 0; i < this.size(); i++) {
			list.add(this.get(i));
		}
		return list;
	}
}
