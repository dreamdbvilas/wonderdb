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

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.IndexKeyType;



public class IndexSerializer {
	private static IndexSerializer instance = new IndexSerializer();
	public static final int BASE_SIZE = BlockPtrSerializer.BASE_SIZE + Integer.SIZE/8;
	
	private IndexSerializer() {
	}
	
	public static IndexSerializer getInstance() {
		return instance;
	}
	
	public int getByteSize(IndexKeyType dt) {
		
		IndexKeyType ikt = null;
		if (dt instanceof IndexKeyType) {
			ikt = (IndexKeyType) dt;
		} else {
			return 0;
		}
		
		int size = BASE_SIZE;
		for (int i = 0; i < ikt.getValue().size(); i++) {
			DBType dbt = ikt.getValue().get(i);
			size = size + 1;
			if (dbt != null) {
				size = size + dbt.getByteSize();
			}
		}
		
		return size;
	}
	
	@SuppressWarnings("unused")
	public void toBytes(SerializableType st, ChannelBuffer buffer, int idxId) {
		IndexKeyType ikt = null;
		if (st instanceof IndexKeyType) {
			ikt = (IndexKeyType) st;
		}
		if (ikt == null) {
			throw new RuntimeException("Null not supported");
		}

		if (ikt.getRecordId() == null) {
			throw new RuntimeException("Record Block connot be null during serialization");
		}
		
		BlockPtr ptr = ikt.getRecordId().getPtr();
		BlockPtrSerializer.getInstance().toBytes(ptr, buffer);
		buffer.writeInt(ikt.getRecordId().getPosn());
		
		List<CollectionColumn> idxColList = SchemaMetadata.getInstance().getIndex(idxId).getIndex().getColumnList();
		List<? extends SerializableType> list = ikt.getValue();
		for (int x = 0; x < ikt.getValue().size(); x++) {
			SerializableType idxCol = list.get(x);
			if (idxCol != null) {
				buffer.writeByte((byte) 0);		
				String s = idxColList.get(x).getSerializerName();
				CollectionColumnSerializer<? extends SerializableType> serializer = SerializerManager.getInstance().getSerializer(idxColList.get(x).getCollectionColumnSerializerName());
				serializer.toBytes(list.get(x), buffer);
			} else {
				buffer.writeByte((byte) 0x01);
			}
		}
	}
	
	public IndexKeyType unmarshal(ChannelBuffer buffer, int schemaObjId) {
		List<DBType> list = new ArrayList<DBType>();
		
		SchemaMetadata meta = SchemaMetadata.getInstance();
		Index idx = meta.getIndex(schemaObjId).getIndex();

		BlockPtr recordPtr = (BlockPtr) BlockPtrSerializer.getInstance().unmarshal(buffer);
		int recordPosn = buffer.readInt();
		
		RecordId recordId = new RecordId(recordPtr, recordPosn);
		
		List<CollectionColumn> cols = idx.getColumnList();
		for (int i = 0; i < cols.size(); i++) {
			byte b = buffer.readByte();
			if ((b | 0x00) == 0) {
				// its not null!
				CollectionColumn col = cols.get(i);
				CollectionColumnSerializer<? extends SerializableType> serializer = SerializerManager.getInstance().getSerializer(col.getCollectionColumnSerializerName());
				SerializableType st = serializer.unmarshal(buffer);
				DBType scct = null;
				if (st instanceof DBType) {
					scct = (DBType) st;
				} else {
					throw new RuntimeException(st + "not instanceof SerializableComparableColumnType");
				}
				list.add(scct);
			} else {
				list.add(null);
			}
		}
		IndexKeyType key = new IndexKeyType(list, recordId);
		return key;
	}
}
