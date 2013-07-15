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
package org.wonderdb.txnlogger.slave;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.Serializer;
import org.wonderdb.seralizers.SerializerManager;
import org.wonderdb.seralizers.StringSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.StringType;

public class SlaveLogManager {
	private static SlaveLogManager instance = new SlaveLogManager();
	private SlaveLogManager() {
	}
	
	public static SlaveLogManager getInstance() {
		return instance;
	}
	
	public void log(ChannelBuffer buffer) {
		buffer.clear();
		long id = buffer.readLong();
		String operation = "";
		String collectionName = "";
		String objectId = "";
		
		StringType st = StringSerializer.getInstance().unmarshal(buffer);
		if (st == null || st.get() == null) {
			return;
		}
		operation = st.get();
		
		st = StringSerializer.getInstance().unmarshal(buffer);
		if (st == null || st.get() == null) {
			return;
		}
		collectionName = st.get();			
		
		st = StringSerializer.getInstance().unmarshal(buffer);
		if (st == null || st.get() == null) {
			return;
		}
		objectId = st.get();			

		Map<String, DBType> map = new HashMap<String, DBType>();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);

		int count = buffer.readInt();
		for (int i = 0; i < count; i++) {
			st = StringSerializer.getInstance().unmarshal(buffer);
			String colName = st.get();
			int colId = colMeta.getColumnId(st.get());
			CollectionColumn cc = colMeta.getCollectionColumn(colId); 
			st = StringSerializer.getInstance().unmarshal(buffer);
			String serName = cc.getCollectionColumnSerializerName();
			Serializer<SerializableType> ser = SerializerManager.getInstance().getSerializer(serName);
			if (ser == null) {
				ser = StringSerializer.getInstance();
			}
			DBType dt = (DBType) ser.unmarshal(buffer);
			map.put(colName, dt);
		}
		Map<ColumnType, DBType> ctMap = TableRecordManager.getInstance().convertTypes(collectionName, map);
		if ("insert".equals(operation)) {
//			try {
//				TableRecordManager.getInstance().addTableRecord(collectionName, ctMap);
//			} catch (InvalidCollectionNameException e) {
//				e.printStackTrace();
//			}
		}

//		if ("update".equals(operation)) {
//			TableRecordManager.getInstance().updateTableRecord(collectionName, map, sh);
//		}
//		
//		if ("delete".equals(operation)) {
//			TableRecordManager.getInstance().delete(collectionName, objectId);
//		}
//		
	}
}
