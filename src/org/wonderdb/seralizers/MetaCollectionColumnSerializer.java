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
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.StringType;


public class MetaCollectionColumnSerializer implements CollectionColumnSerializer<CollectionColumn> {
	private static MetaCollectionColumnSerializer instance = new MetaCollectionColumnSerializer();
	
	private MetaCollectionColumnSerializer() {
	}
	
	public static MetaCollectionColumnSerializer getInstance() {
		return instance;
	}
	
	@Override
	public void toBytes(SerializableType o, ChannelBuffer buffer) {
		CollectionColumn cc = null;
		if (o instanceof CollectionColumn) {
			cc = (CollectionColumn) o;
		} else {
			return;
		}
		buffer.writeInt(cc.getColumnId());
		buffer.writeByte(cc.isNullable() ? (byte) 1 : (byte) 0);
		StringSerializer.getInstance().toBytes(new StringType(cc.getColumnName()), buffer);
		StringSerializer.getInstance().toBytes(new StringType(cc.getCollectionColumnSerializerName()), buffer);
		buffer.writeByte(cc.isQueriable() ? (byte) 1 : (byte) 0);
	}

	@Override
	public SerializableType unmarshal(ChannelBuffer buffer) {
		int id = buffer.readInt();
		boolean isNull = buffer.readByte() == 1 ? true : false;
		StringType name = StringSerializer.getInstance().unmarshal(buffer);
		StringType serName = StringSerializer.getInstance().unmarshal(buffer);
		boolean isQUeriable = buffer.readByte() == 1 ? true : false;
		return new CollectionColumn(name.get(), id, serName.get(), isNull, isQUeriable);		
	}

	@Override
	public CollectionColumn getValueObject(String s) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType getDBObject(DBType s) {
		if (s instanceof CollectionColumn) {
			return s;
		}
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getByteSize(CollectionColumn dt) {
		return dt != null ? dt.getByteSize() : 0;
	}

	@Override
	public int getSQLType() {
		throw new RuntimeException("Method not supported");
	}
}
