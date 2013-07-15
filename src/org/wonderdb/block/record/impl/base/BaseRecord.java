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
package org.wonderdb.block.record.impl.base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.query.parse.DBSelectQuery;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.seralizers.CollectionColumnSerializer;
import org.wonderdb.seralizers.SerializerManager;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.StringType;


public class BaseRecord implements QueriableBlockRecord {
	RecordId recordId = null;
//	int posn;
	Map<ColumnType, DBType> map = new HashMap<ColumnType, DBType>();
	Set<ColumnType> removeList = null;
	long lastAccessed = -1;
	long lastUpdated = -1;
	int bufferCount = 0;
	
	private static int BASE_SIZE = 2*Integer.SIZE/8;
	
	public BaseRecord() {	
	}
	
	public BaseRecord(RecordId recordId) {
		this.recordId = recordId;
	}
	
	public BaseRecord(Map<ColumnType, DBType> map) {
		this.map.putAll(map);
	}
	
//	public int getPosn() {
//		return posn;
//	}
	
//	public void setPosn(int p) {
//		posn = p;
//	}	
	
	@Override
	public Map<ColumnType, DBType> getColumns() {
		return getQueriableColumns();
	}
	
	
	@Override
	public void setColumns(Map<ColumnType, DBType> colValues) {
		map.putAll(colValues);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public DBType getColumnValue(CollectionMetadata colMeta, ColumnType columnType, String path) {
		DBType value = map.get(columnType);
		if (colMeta == null) {
			return value;
		}
		if (columnType.getValue() instanceof String) {
			return new StringType((String) columnType.getValue());
		}
		CollectionColumn cc = colMeta.getCollectionColumn((Integer) columnType.getValue());
		String columnName = null;
		if (path != null) {
			columnName = path;
		} else {
			columnName = cc.getColumnName();
		}
		String[] split = columnName.split("://");
		if (split.length == 1) {
			return value;
		}
		
		if (split.length == 2) {
			String protocol = split[0];
			String expression = "/" + split[1].substring(split[1].indexOf('/')+1);
			CollectionColumnSerializer<DBType> serializer = (CollectionColumnSerializer<DBType>) SerializerManager.getInstance().getSerializer(cc.getCollectionColumnSerializerName());
			String colName = DBSelectQuery.extractColumnName(columnName);
			String rootNode = DBSelectQuery.extractRootNode(columnName);
			ColumnType ct = colMeta.getColumnType(colName);
			DBType val = map.get(ct);
			RecordValueExtractor rve = RecordValueProtocolManager.getInstance().getRecordValueExtractor(protocol);
			StringType v = (StringType) rve.extract(expression, val, rootNode);
			return serializer.getDBObject(v);
		}
		return null;
	}
	
	@Override
	public void setColumnValue(ColumnType name, DBType value) {
		map.put(name, value);
	}
	
	@Override
	public void removeColumn(ColumnType name) {
		if (removeList == null) {
			removeList = new HashSet<ColumnType>();
		}
		removeList.add(name);
	}

	@Override
	public Map<ColumnType, DBType> getQueriableColumns() {
		
		return new HashMap<ColumnType, DBType>(map);
//		Iterator<ColumnType> iter = map.keySet().iterator();
//		while (iter.hasNext()) {
//			ColumnType ct = iter.next();
//			DBType dt = map.get(ct);
//			if (dt instanceof CollectionColumn) {
//				CollectionColumn cc = (CollectionColumn) dt;
//				if (cc.isNullable()) {
//					c.put(ct, dt);
//				}
//			} else {
//				c.put(ct, dt);
//			}
//		}
//		return c;
	}	
	
	public Set<ColumnType> getLoadedColumns(Set<ColumnType> columnsToLoad) {
		Set<ColumnType> set = new HashSet<ColumnType>(columnsToLoad);
		Set<ColumnType> loadedColumns = map.keySet();
		set.removeAll(loadedColumns);
		set.removeAll(removeList);
		return set;
	}

	@Override
	public String getSerializerName() {
		return null;
	}

	@Override
	public int getByteSize() {
		int size = BASE_SIZE;
		Iterator<DBType> iter = map.values().iterator();
		while (iter.hasNext()) {
			DBType dt = iter.next();
			if (dt == null) {
				continue;
			}
			size = size + dt.getByteSize();
		}
		size = size + (Integer.SIZE/8 + 1) * map.size();
		return size;
	}

	@Override
	public long getLastAccessDate() {
		return lastAccessed;
	}

	@Override
	public long getLastModifiedDate() {
		return lastUpdated;
	}

	@Override
	public void updateLastAccessDate() {
		lastAccessed = System.currentTimeMillis();
	}

	@Override
	public void updateLastModifiedDate() {
		lastUpdated = System.currentTimeMillis();
		lastAccessed = lastUpdated;
	}

	@Override
	public RecordId getRecordId() {
		return recordId;
	}
	
	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}

	@Override
	public int getBufferCount() {
		return bufferCount;
	}

	@Override
	public void setBufferCount(int count) {
		this.bufferCount = count;
	}

	@Override
	public Map<ColumnType, DBType> copyAndGetColumns(List<ColumnType> selectColums) {
		Map<ColumnType, DBType> retMap = new HashMap<ColumnType, DBType>();
		Iterator<ColumnType> iter = map.keySet().iterator();
		while (iter.hasNext()) {
			ColumnType ct = iter.next();
			DBType dt = map.get(ct);
			if (dt != null) {
				if (selectColums == null || selectColums.contains(ct)) {
					DBType newDt = dt.copyOf();
					retMap.put(ct, newDt);
				}
			}
		}
		return retMap;
	}
}
