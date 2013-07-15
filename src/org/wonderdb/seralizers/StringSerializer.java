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
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.StringType;



public class StringSerializer implements CollectionColumnSerializer<StringType> {
	private static StringSerializer instance = new StringSerializer();
	private static final int BASE_SIZE = Integer.SIZE/8;
	
	private StringSerializer() {
	}
	
	public static StringSerializer getInstance() {
		return instance;
	}
	
	@Override
	public int getByteSize(StringType st) {
		if (st == null) {
			throw new RuntimeException("Null not supported");
		}
		int strLen = 0;
		if (st.get() != null) {
			strLen = st.get().getBytes().length; 
		}
		return strLen + BASE_SIZE;
	}
	
	@Override
	public StringType getValueObject(String s) {
		return new StringType(s); 
	}

	@Override
	public DBType getDBObject(DBType s) {
		if (s == null) {
			return null;
		}
		if (s instanceof StringType) {
			StringType st = (StringType) s;
			String value = st.get();
			if (value == null) {
				return null;
			}
			return new StringType(value);
		}
		
		if (s instanceof CollectionColumn) {
			return s;
		}
		
		throw new RuntimeException("null not supported");
	}
	
	@Override
	public void toBytes(SerializableType t, ChannelBuffer buffer) {
		StringType st = null;
		if (t instanceof StringType) { 
			st = (StringType) t;
		} else {
			throw new RuntimeException("Invalid type or null: " + st);
		}
		
		String s = st.get();
		if (s == null) {
			s = "";
		}
//		buffer.resetWriterIndex();
		buffer.writeInt(s.getBytes().length);
		buffer.writeBytes(s.getBytes());
	}
	
	@Override
	public StringType unmarshal(ChannelBuffer buffer) {
		String s = "";
		int len = buffer.readInt();
		byte[] bytes = new byte[len];
		buffer.readBytes(bytes);
		s = new String(bytes);
		StringType i = new StringType(s);
		return i;
	}

	@Override
	public int getSQLType() {
		return Types.VARCHAR;
	}
}
