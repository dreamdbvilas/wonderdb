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
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.ColumnTypeHelper;
import org.wonderdb.types.impl.RecordValuesType;


public class RecordValuesTypeSerializer {
	// singleton
	private static final RecordValuesTypeSerializer instance = new RecordValuesTypeSerializer();
	
	// singleton
	private RecordValuesTypeSerializer() {
	}
	
	// singleton
	public static RecordValuesTypeSerializer getInstance() {
		return instance;
	}
	
	public void toBytes(RecordValuesType rvt, ChannelBuffer buffer) {
		if (rvt == null) {
			return;
		}
		
		buffer.clear();
		List<? extends SerializableType> list = rvt.getValues();
		for (int i = 0; i < list.size(); i++) {
			SerializableType st = list.get(i);
			CollectionColumnSerializer<? extends SerializableType> serializer = SerializerManager.getInstance().getSerializer(st.getSerializerName());
			serializer.toBytes(st, buffer);
		}
	}
	
	public RecordValuesType unmarshal(ChannelBuffer buffer, List<ColumnType> columnList, int schemaId) {
		List<SerializableType> list = new ArrayList<SerializableType>(columnList.size());
		for (int i = 0; i < columnList.size(); i++) {
			ColumnType ct = columnList.get(i);
			CollectionColumnSerializer<? extends SerializableType> ser = ColumnTypeHelper.getInstance().getSerializer(ct, schemaId);
			SerializableType st = ser.unmarshal(buffer);
			list.add(st);
		}
		
		return new RecordValuesType(list);
	}
}
