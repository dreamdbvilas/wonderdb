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
import org.wonderdb.types.impl.LongType;
import org.wonderdb.types.impl.StringType;


public class LongSerializer implements CollectionColumnSerializer<LongType> {
	private static LongSerializer instance = new LongSerializer();
	private LongSerializer() {
	}
	
	static public LongSerializer getInstance() {
		return instance;
	}
	
	@Override
	public int getByteSize(LongType dt) {
		return Long.SIZE/8;
	}
	
	@Override
	public LongType getValueObject(String s) {
		long value = Long.valueOf(s);
		return new LongType(value); 
	}
	
	@Override
	public DBType getDBObject(DBType s) {
		if (s == null) {
			return null;
		}
		
		if (s instanceof StringType) {
			StringType st = (StringType) s; 
			if (st.get() == null) {
				return null;
			}
			
			long value = 0;
			try {
				value = Long.valueOf(st.get());
			} catch (NumberFormatException e) {
				return null;
			}
			return new LongType(value);
		}
		
		if (s instanceof LongType) {
			return s;
		}
		
		throw new RuntimeException("null not supported");
	}
	
	@Override
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		LongType lt = null;
		if (st instanceof LongType) {
			lt = (LongType) st;
		} else {
			throw new RuntimeException("Invalid type or null: " + st);
		}
		
		long s = lt.get();
		buffer.writeLong(s);
	}
	
	@Override
	public LongType unmarshal(ChannelBuffer buffer) {
		long val = -1;
		val = buffer.readLong();
		LongType i = new LongType(val);
		return i;
	}

	@Override
	public int getSQLType() {
		return Types.NUMERIC;
	}
}
