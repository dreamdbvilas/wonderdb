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
import org.wonderdb.types.impl.FloatType;
import org.wonderdb.types.impl.StringType;


public class FloatSerializer implements CollectionColumnSerializer<FloatType> {
	private static FloatSerializer instance = new FloatSerializer();
	private FloatSerializer() {
	}
	
	public static FloatSerializer getInstance() {
		return instance;
	}
	
	@Override
	public int getByteSize(FloatType dt) {
		return Float.SIZE/8;
	}
	
	@Override
	public FloatType getValueObject(String s) {
		float value = Float.valueOf(s);
		return new FloatType(value); 
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
			float value = Float.valueOf(st.get());
			return new FloatType(value);
		}
		
		if (s instanceof FloatType) {
			return s;
		}
		
		throw new RuntimeException("null not supported");
	}
	
	@Override
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		FloatType ft = null;
		
		if (st instanceof FloatType) {
			ft = (FloatType) st;
		} else {
			throw new RuntimeException("Invalid type or null: " + st);
		}
		
		float s = ft.get();
		buffer.writeFloat(s);
	}
	
	@Override
	public FloatType unmarshal(ChannelBuffer buffer) {
		float val = -1;
		val = buffer.readFloat();
		FloatType i = new FloatType(val);
		return i;
	}

	@Override
	public int getSQLType() {
		return Types.FLOAT;
	}
}
