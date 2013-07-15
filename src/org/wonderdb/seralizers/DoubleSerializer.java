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
import org.wonderdb.types.impl.DoubleType;
import org.wonderdb.types.impl.StringType;



public class DoubleSerializer implements CollectionColumnSerializer<DoubleType> {
	private static DoubleSerializer instance = new DoubleSerializer();
	private DoubleSerializer() {
	}
	
	public static DoubleSerializer getInstance() {
		return instance;
	}
	
	@Override
	public int getByteSize(DoubleType dt) {
		return Double.SIZE/8;
	}
	
	@Override
	public void toBytes(SerializableType st, ChannelBuffer buffer) {
		DoubleType dt = null;
		if (st instanceof DoubleType) {
			dt = (DoubleType) st;
		} else {
			throw new RuntimeException("Invalid type or null: " + st);
		}
		double s = dt.get();
		buffer.writeDouble(s);
	}
	
	@Override
	public DoubleType getValueObject(String s) {
		double value = Double.valueOf(s);
		return new DoubleType(value); 
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
			double value = Double.valueOf(st.get());
			return new DoubleType(value);
		}
		
		if (s instanceof DoubleType) {
			return s;
		}
		
		throw new RuntimeException("null not supported");

	}
	
	@Override
	public DoubleType unmarshal(ChannelBuffer buffer) {
		double val = -1;
		val = buffer.readDouble();
		DoubleType i = new DoubleType(val);
		return i;
	}

	@Override
	public int getSQLType() {
		return Types.DOUBLE;
	}
}
