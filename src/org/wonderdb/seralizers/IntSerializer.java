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
import org.wonderdb.types.impl.IntType;
import org.wonderdb.types.impl.StringType;



public class IntSerializer implements CollectionColumnSerializer<IntType> {
	private static IntSerializer instance = new IntSerializer();
	
	private IntSerializer() {
	}
	
	public static IntSerializer getInstance() {
		return instance;
	}
	
	@Override
	public int getByteSize(IntType dt) {
		return Integer.SIZE/8;
	}
	
	@Override
	public IntType getValueObject(String s) {
		int value = Integer.valueOf(s);
		return new IntType(value); 
	}

	@Override
	public DBType getDBObject(DBType s) {
		if (s == null) {
			return null;
		}
		
		if (s instanceof StringType) {
			StringType st = (StringType) s;
			if (st == null || st.get() == null) {
				return null;
			}
			int value = Integer.valueOf(st.get());
			
			return new IntType(value);
		}
		
		if (s instanceof IntType) {
			return s;
		}

		throw new RuntimeException("null not supported");
	}
	

	@Override
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		IntType it = null;
		
		if (st instanceof IntType) {
			it = (IntType) st;
		} else {
			throw new RuntimeException("Invalid type or null: " + st);
		}
		
		buffer.writeInt(it.get());
	}
	
	@Override
	public IntType unmarshal(ChannelBuffer buffer) {
		int val = -1;
		val = buffer.readInt();
		IntType i = new IntType(val);
		return i;
	}

	@Override
	public int getSQLType() {
		return Types.INTEGER;
	}
}
