package org.wonderdb.serialize;
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

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexRecordMetadata;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;



public class IndexSerializer implements TypeSerializer {
	public static IndexKeyType NULL_INDEX_KEY_TYPE = new IndexKeyType(null, null);
	
	private static IndexSerializer instance = new IndexSerializer();
	public static final int BASE_SIZE = 9 + Integer.SIZE/8;
	
	private IndexSerializer() {
	}
	
	public static IndexSerializer getInstance() {
		return instance;
	}
	
	@Override
	public int getSize(DBType dt, TypeMetadata meta) {
		IndexKeyType ikt = (IndexKeyType) dt;
		
		List<Integer> columnTypes = (( IndexRecordMetadata) meta).getTypeList();
		
		int size = BASE_SIZE;
		for (int i = 0; i < ikt.getValue().size(); i++) {
			DBType dbt = ikt.getValue().get(i);
			size = size + 1;
			if (dbt != null) {
				size = size + SerializerManager.getInstance().getSerializer(columnTypes.get(i)).getSize(dbt, meta);
			}
		}
		
		return size;
	}
	
	@Override
	public void toBytes(DBType st, ChannelBuffer buffer, TypeMetadata meta) {
		IndexRecordMetadata irm = (IndexRecordMetadata) meta;
		List<Integer> columnTypes = ((IndexRecordMetadata) meta).getTypeList();
		IndexKeyType ikt = null;
		if (st instanceof IndexKeyType) {
			ikt = (IndexKeyType) st;
		}
		if (ikt == null) {
			throw new RuntimeException("Null not supported");
		}

		if (ikt.getRecordId() == null && irm.getColumnIdList() != irm.getTypeList()) {
			throw new RuntimeException("Record Block connot be null during serialization");
		}
		
		if (irm.getColumnIdList() != irm.getTypeList()) {			
			BlockPtr ptr = ikt.getRecordId().getPtr();
			SerializerManager.getInstance().getSerializer(SerializerManager.BLOCK_PTR).toBytes(ptr, buffer, meta);
			buffer.writeInt(ikt.getRecordId().getPosn());
		}
		
		for (int x = 0; x < ikt.getValue().size(); x++) {
			DBType dt = ikt.getValue().get(x);
			SerializerManager.getInstance().getSerializer(columnTypes.get(x)).toBytes(dt, buffer, meta);
		}
	}
	
	@Override
	public DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta) {
		IndexRecordMetadata irm = (IndexRecordMetadata) meta;
		List<Integer> columnTypes = ((IndexRecordMetadata) meta).getTypeList();
		List<DBType> list = new ArrayList<DBType>();

		RecordId recordId = null;
		
		if (irm.getColumnIdList() != irm.getTypeList()) {			
			BlockPtr recordPtr = (BlockPtr) SerializerManager.getInstance().getSerializer(SerializerManager.BLOCK_PTR).unmarshal(SerializerManager.BLOCK_PTR, buffer, meta);
			int recordPosn = buffer.readInt();
			
			recordId = new RecordId(recordPtr, recordPosn);
		}
		
		for (int i = 0; i < columnTypes.size(); i++) {
			DBType dt = SerializerManager.getInstance().getSerializer(columnTypes.get(i)).unmarshal(columnTypes.get(i), buffer, meta);
			list.add(dt);
		}
		IndexKeyType key = new IndexKeyType(list, recordId);
		return key;
	}

	@Override
	public boolean isNull(int type, DBType object) {
		return object == null || NULL_INDEX_KEY_TYPE.equals(object);
	}

	@Override
	public DBType getNull(int type) {
		return NULL_INDEX_KEY_TYPE;
	}

	@Override
	public int getSQLType(int type) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType convert(int type, StringType st) {
		throw new RuntimeException("Method not supported");
	}
}
