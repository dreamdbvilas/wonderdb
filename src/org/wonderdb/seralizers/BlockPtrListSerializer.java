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
package org.wonderdb.seralizers;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtrList;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;


public class BlockPtrListSerializer implements CollectionColumnSerializer<SingleBlockPtrList> {
	public static final int BASE_SIZE = 1 + Long.SIZE/8;
	private static BlockPtrListSerializer instance = new BlockPtrListSerializer();
	
	private BlockPtrListSerializer() {
	}
	
	public static BlockPtrListSerializer getInstance() {
		return instance;
	}
	
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		SingleBlockPtrList list = null;
		if (st instanceof SingleBlockPtrList) {
			list = (SingleBlockPtrList) st;
		}

		buffer.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			BlockPtr ptr = list.get(i);
			BlockPtrSerializer.getInstance().toBytes(ptr, buffer);
		}
	}

	public SingleBlockPtrList unmarshal(ChannelBuffer buffer) {
		int size = buffer.readInt();
		SingleBlockPtrList list = new SingleBlockPtrList();
		for (int i = 0; i < size; i++) {
			list.add(BlockPtrSerializer.getInstance().unmarshal(buffer));
		}
		return list;
	}
	
	@Override
	public SingleBlockPtrList getValueObject(String s) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType getDBObject(DBType s) {
		if (s instanceof SingleBlockPtrList) {
			return s;
		}
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getSQLType() {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getByteSize(SingleBlockPtrList dt) {
		if (dt == null) {
			return 0;
		}
		return dt.getByteSize();
	}
}
