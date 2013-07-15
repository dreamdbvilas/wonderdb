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
package org.wonderdb.types.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.CollectionColumnSerializer;
import org.wonderdb.seralizers.SerializerManager;
import org.wonderdb.seralizers.StringSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;


public class ColumnTypeHelper {
	// singleton
	private static ColumnTypeHelper instance = new ColumnTypeHelper();
	
	// singleton
	private ColumnTypeHelper() {
	}
	
	// singleton
	public static ColumnTypeHelper getInstance() {
		return instance;
	}
	
	public CollectionColumnSerializer<? extends SerializableType> getSerializer(ColumnType ct, int schemaId) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
		CollectionColumnSerializer<? extends SerializableType> ser = StringSerializer.getInstance();
		if (ct.getValue() instanceof String) {
			return ser;
		}
		String serName = colMeta.getColumnSerializerName(ct);
		if (serName != null && serName.length() > 0) {
			ser = SerializerManager.getInstance().getSerializer(serName);
		}
		return ser;
	}
	
	public ColumnType getColumnType(CollectionMetadata colMeta, String s) {
		Integer id = colMeta.getColumnId(s);
		if (id == null) {
			return new ColumnType(s);
		}
		return new ColumnType(id);
	}
	
	public Map<ColumnType, DBType> convertToColumnTypeMap(CollectionMetadata colMeta, Map<String, DBType> map) {
		Map<ColumnType, DBType> retMap = new HashMap<ColumnType, DBType>();
		Iterator<String> iter = map.keySet().iterator();
		while (iter.hasNext()) {
			String s = iter.next();
			ColumnType ct = getColumnType(colMeta, s);
			retMap.put(ct, map.get(s));
		}
		return retMap;
	}
}
