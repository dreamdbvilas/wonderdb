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
package org.wonderdb.cluster.kafka;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.CollectionColumnSerializer;
import org.wonderdb.seralizers.SerializerManager;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.ColumnTypeHelper;

public class KafkaSerializer implements Decoder<KafkaPayload>, Encoder<KafkaPayload> {
	
	public KafkaSerializer(VerifiableProperties props) {
	}
	
	public void init(VerifiableProperties properties) {
		
	}
	
	@Override
	public byte[] toBytes(KafkaPayload payload) {
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		buffer.writeInt(payload.queryType.getBytes().length);
		buffer.writeBytes(payload.queryType.getBytes());
		buffer.writeInt(payload.collectionName.getBytes().length);
		buffer.writeBytes(payload.collectionName.getBytes());
		buffer.writeInt(payload.objectId.getBytes().length);
		buffer.writeBytes(payload.objectId.getBytes());
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(payload.collectionName);
		
		if (payload.map != null && payload.map.size() > 0) {
			buffer.writeInt(payload.map.size());
			Iterator<ColumnType> iterator = payload.map.keySet().iterator();
			while (iterator.hasNext()) {
				ColumnType k = iterator.next();
				DBType val = payload.map.get(k);
				String key = colMeta.getColumnName((Integer) k.getValue()); 
				buffer.writeInt(key.getBytes().length);
				buffer.writeBytes(key.getBytes());
				if (val == null) {
					buffer.writeByte(0);
				} else {
					buffer.writeByte(1);
					CollectionColumnSerializer<? extends SerializableType> ser = SerializerManager.getInstance().getSerializer(val.getSerializerName());
					ser.toBytes(val, buffer);
				}
			}
		}
		byte[] bytes = new byte[buffer.writerIndex()];
		buffer.readBytes(bytes);
		return bytes;
	}

	@Override
	public KafkaPayload fromBytes(byte[] arg0) {
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(arg0);
		int size = buffer.readInt();
		byte[] bytes = new byte[size];
		buffer.readBytes(bytes);
		String queryType = new String(bytes);
		
		size = buffer.readInt();
		bytes = new byte[size];
		buffer.readBytes(bytes);
		String collectionName = new String(bytes);
		
		size = buffer.readInt();
		bytes = new byte[size];
		buffer.readBytes(bytes);
		String objectId = new String(bytes);
		CollectionMetadata colMetadata = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		int schemaId = colMetadata.getSchemaId();

		Map<ColumnType, DBType> map = null;
		if (buffer.readerIndex() < buffer.capacity()) {
			int mapSize = buffer.readInt();
			map = new HashMap<ColumnType, DBType>(mapSize);
			for (int i = 0; i < mapSize; i++) {
				int sz = buffer.readInt();
				bytes = new byte[sz];
				buffer.readBytes(bytes);
				String colName = new String(bytes);
				ColumnType ct = colMetadata.getColumnType(colName);
				DBType val = null;
				
				byte b = buffer.readByte();
				if (b != 0) {
					CollectionColumnSerializer<? extends SerializableType> ser = ColumnTypeHelper.getInstance().getSerializer(ct, schemaId);
					val = (DBType) ser.unmarshal(buffer);
				}
				map.put(ct, val);
			}
		}
		
		return new KafkaPayload(queryType, collectionName, objectId, map);
	}
}
