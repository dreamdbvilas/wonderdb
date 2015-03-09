package org.wonderdb.schema;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.cluster.Shard;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.types.ColumnNameMeta;




public class CollectionMetadata  {
	public void setLoggingEnabled(boolean isLoggingEnabled) {
		this.isLoggingEnabled = isLoggingEnabled;
	}

	ConcurrentMap<Integer, ColumnNameMeta> columnIdToNameMap = new ConcurrentHashMap<Integer, ColumnNameMeta>();
	ConcurrentMap<String, ColumnNameMeta> columnNameToIdMap = new ConcurrentHashMap<>();
	
	ConcurrentMap<Shard, WonderDBList> shardRecordListMap = new ConcurrentHashMap<Shard, WonderDBList>();
	WonderDBList dbList = null;
	
	boolean isLoggingEnabled = false;
	String collectionName = null;
	
	public CollectionMetadata(String collectionName) {
		this.collectionName = collectionName;
	}
	
	public synchronized boolean contains(Shard shard) {
		return shardRecordListMap.containsKey(shard);
	}
	
	public synchronized List<ColumnNameMeta> getCollectionColumns() {
		return new ArrayList<>(columnIdToNameMap.values());
	}
	
	public List<ColumnNameMeta> addColumns(Collection<ColumnNameMeta> columns) {
		List<ColumnNameMeta> list = new ArrayList<ColumnNameMeta>();
		if (columns == null) {
			return list;
		}
		
		Iterator<ColumnNameMeta> iter = columns.iterator();
		while (iter.hasNext()) {
			ColumnNameMeta col = iter.next();
			ColumnNameMeta cnm = add(col);
			if (cnm != null) {
				list.add(cnm);
			}
		}
		return list;
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public ColumnNameMeta add(ColumnNameMeta column) {
		if (!this.columnNameToIdMap.containsKey(column.getColumnName())) {
			column.setCoulmnId(columnNameToIdMap.size());
			this.columnNameToIdMap.put(column.getColumnName(), column);
			this.columnIdToNameMap.put(column.getCoulmnId(), column);
			return column;
		} 
		return null;
	}
	
	public ColumnNameMeta getCollectionColumn(int colId) {
		return columnIdToNameMap.get(colId);
	}
	
	public Integer getColumnId(String columnName) {
		ColumnNameMeta cnm = columnNameToIdMap.get(columnName);
		return cnm != null ? cnm.getCoulmnId() : -1;
	}
	
	public String getColumnName(int id) {
		ColumnNameMeta cnm = columnIdToNameMap.get(id);
		return cnm != null ? cnm.getColumnName() : null;
	}	
	
	public int getColumnType(String name) {
		ColumnNameMeta cnm = columnNameToIdMap.get(name);
		return cnm != null ? cnm.getColumnType() : -1;
	}
	
	
	public WonderDBList getRecordList(Shard shard) {
		
//		return shardRecordListMap.get(shard);
		return dbList;
	}	
	
	public boolean isLoggingEnabled() {
		return isLoggingEnabled;
	}
	
	public List<Shard> getShards() {
		return new ArrayList<Shard>(shardRecordListMap.keySet());
	}
	
	public void setDBList(WonderDBList list) {
		this.dbList = list;
	}
	
}
