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

import java.sql.Types;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ByteType;


public class ByteSerializer implements CollectionColumnSerializer<ByteType> {
	public static final int BASE_SIZE = Integer.SIZE/8;
	
	private static ByteSerializer instance = new ByteSerializer();
	
	private ByteSerializer() {
	}
	
	public static ByteSerializer getInstance() {
		return instance;
	}
	
	@Override
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		ByteType bt = null;
		if (st instanceof ByteType) {
			bt = (ByteType) st;
		} else {
			throw new RuntimeException("Invalid type (or null): " + st);
		}

		buffer.writeInt(bt.get().length);
		buffer.writeBytes(bt.get());
	}
	
	@Override
	public ByteType unmarshal(ChannelBuffer buffer) {
		int size = buffer.readInt();
		byte[] bytes = null;
		if (size > 0) {
			bytes = new byte[size];
			buffer.readBytes(bytes);
		}
		return new ByteType(bytes);
	}
	
	@Override
	public ByteType getValueObject(String s) {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public DBType getDBObject(DBType s) {
		if (s instanceof ByteType) {
			return s;
		}
		throw new RuntimeException("Method not supported");		
	}
	
	@Override
	public int getByteSize(ByteType dt) {
		if (dt == null) {
			throw new RuntimeException("null not supported");
		}
		int size = dt.get().length;
		return BASE_SIZE + size;
	}

	@Override
	public int getSQLType() {
		return Types.BINARY;
	}

}
