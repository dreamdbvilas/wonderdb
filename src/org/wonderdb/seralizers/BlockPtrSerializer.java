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
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;


public class BlockPtrSerializer implements CollectionColumnSerializer<BlockPtr> {
	public static final int BASE_SIZE = 1 + Long.SIZE/8;
	private static BlockPtrSerializer instance = new BlockPtrSerializer();
	
	private BlockPtrSerializer() {
	}
	
	public static BlockPtrSerializer getInstance() {
		return instance;
	}
	
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		BlockPtr ptr = null;
		if (st instanceof BlockPtr) {
			ptr = (BlockPtr) st;
		}
		long blockPosn = -1;
		byte fileId = -1;
		if (ptr != null) {
			blockPosn = ptr.getBlockPosn();
			fileId = ptr.getFileId();
		}
		
		buffer.writeLong(blockPosn);
		buffer.writeByte(fileId);
	}

	public BlockPtr unmarshal(ChannelBuffer buffer) {
		try {
			long posn = buffer.readLong();
			byte fileId = buffer.readByte();
			return new SingleBlockPtr(fileId, posn);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public int getByteSize(BlockPtr dt) {
		return BASE_SIZE;
	}

	@Override
	public BlockPtr getValueObject(String s) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType getDBObject(DBType s) {
		if (s instanceof BlockPtr) {
			return s;
		}
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getSQLType() {
		throw new RuntimeException("Method not supported");
	}
}
