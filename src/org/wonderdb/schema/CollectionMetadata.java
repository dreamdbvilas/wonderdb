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
package org.wonderdb.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.impl.QueriableRecordList;
import org.wonderdb.collection.impl.QueriableRecordListImpl;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;



public class CollectionMetadata extends SchemaObjectImpl {
	long count=0;
	boolean memoryOnly = true;
//	Map<ColumnType, DBType> queriableColumns = new HashMap<ColumnType, DBType>();
	Map<ColumnType, String> colToSerializerNameMap = new HashMap<ColumnType, String>();
	Map<Integer, String> columnIdToNameMap = new HashMap<Integer, String>();
	Map<String, Integer> columnNameToIdMap = new HashMap<String, Integer>();
	ConcurrentMap<Shard, QueriableRecordList> shardRecordListMap = new ConcurrentHashMap<Shard, QueriableRecordList>();
//	QueriableRecordList recordList = null;
	boolean isLoggingEnabled = false;
	
	Block parentBlock = null;
	
//	public CollectionMetadata (String collectionName, int id) {
//		this(collectionName, false, id);
//	}
	
	public CollectionMetadata (String collectionName, int id) {
		super(collectionName, "TS", id);
	}

	public CollectionMetadata (String collectionName, boolean memoryOnly, int id) {
		super(collectionName, "TS", id);
		this.memoryOnly = memoryOnly;
	}
	
//	public CollectionMetadata (String collectionName, boolean memoryOnly, int id, BlockPtr headPtr) {
//		super(collectionName, "TS", id);
//		this.memoryOnly = memoryOnly;
//	}
	
	public void incrementCount() {
		count++;
	}
	
	public long getCount() {
		return count;
	}
	
//	public synchronized List<CollectionColumn> getCollectionColumns() {
//		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
//		Iterator<Integer> iter = columnIdToNameMap.keySet().iterator();
//		while (iter.hasNext()) {
//			int id = iter.next();
//			String name = columnIdToNameMap.get(id);
//			CollectionColumn cc = new CollectionColumn(name, id, colToSerializerNameMap.get(new ColumnType(id)), true);
//			list.add(cc);
//		}
//		return list;
//	}
//	
	public synchronized List<CollectionColumn> getCollectionColumns() {
		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
		Iterator<DBType> iter = getQueriableColumns().values().iterator();
		while (iter.hasNext()) {
			CollectionColumn c = (CollectionColumn) iter.next();
			CollectionColumn cc = new CollectionColumn(c.getColumnName(), c.getColumnId(), c.getCollectionColumnSerializerName(), c.isNullable(), c.isQueriable());
			list.add(cc);
		}
		return list;
	}
	
	public void setCount(long c) {
		count = c;
	}
	
	public List<CollectionColumn> addColumns(Collection<CollectionColumn> columns) {
		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
		if (columns == null) {
			return list;
		}
		
		Iterator<CollectionColumn> iter = columns.iterator();
		while (iter.hasNext()) {
			CollectionColumn col = iter.next();
			if (add(col) != null) {
				list.add(col);
//				getColumns().put(col.getColumnType(), col.g)
			}
		}
		return list;
	}
	
	public CollectionColumn add(String column) {
		CollectionColumn cc = new CollectionColumn(this, column, "ss");
		return add (cc);
	}
	
	public CollectionColumn getCollectionColumn(int colId) {
		CollectionColumn cc = (CollectionColumn) getColumnValue(null, new ColumnType(colId), null);
//
//		CollectionMetadata mColMeta = SchemaMetadata.getInstance().getCollectionMetadata("metaCollection");
//		SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
//		scm.addColumn(cc);
//		TableRecord record = new TableRecord(scm.getColumns());
//		mColMeta.getRecordList().update(getRecordId(), record);
		return cc;
	}
	
	public CollectionColumn add(CollectionColumn cc) {
//		ColumnType ct = cc.getColumnType();
//		if (ct.getValue() instanceof Integer) {
//			int i = (Integer) ct.getValue();
//			if (i >= 0 && i < 10) {
//				return cc;
//			}
//		}
		
		if (columnNameToIdMap.containsKey(cc.columnName)) {
			return null;
		}
		if (getColumnValue(null, cc.getColumnType(), null) == null) {
			int colId = -1;
			if (cc.columnId < 0) {
				colId = columnIdToNameMap.size();
			} else {
				colId = cc.columnId;
			}
			cc.columnId = colId;
			columnIdToNameMap.put(colId, cc.getColumnName());
			columnNameToIdMap.put(cc.getColumnName(), colId);
			setColumnValue(cc.getColumnType(), cc);
			colToSerializerNameMap.put(cc.getColumnType(), cc.serializerName);
			return cc;
		}		
		return null;
	}
	
//	public synchronized List<ColumnType> getQueriableColumns() {
//		return new ArrayList<ColumnType>(queriableColumns.keySet());
//	}
//	
	public String getColumnSerializerName(ColumnType columnType) {
		if (schemaId == 1) {
			if (columnType.getValue() instanceof Integer) {
				int i = (Integer) columnType.getValue();
				if (i >= 3) {
					return "cc";
				}
			}
		}
		return colToSerializerNameMap.get(columnType);
	}
	
//	public String getColumnSerializerName(int colId) {
//		String colName = columnIdToNameMap.get(colId);
//		return colToSerializerNameMap.get(colName);
//	}
//	
	public boolean getMemoryOnly() {
		return memoryOnly;
	}
	
	public String getSerializerName() {
		return "cms";
	}
	
	public void updateContents(Object o) {
	}

//	public int getByteSize() {
//		return 0;
////		return CollectionMetadataSerializer.getInstance().getByteSize(this);
//	}
//	
	public Integer getColumnId(String columnName) {
		Integer i = columnNameToIdMap.get(columnName);
		if (i == null) {
			return -1;
		}
		return i;
	}
	
	public String getColumnName(int id) {
		return columnIdToNameMap.get(id);
	}	
	
	public ColumnType getColumnType(String name) {
		ColumnType ct = null;
		int id = getColumnId(name);
		if (id < 0) {
			ct = new ColumnType(name);
		} else {
			ct = new ColumnType(id);
		}
		return ct;
	}
	
	public void createRecordList(Shard shard) {
		if (shard == null) {
			shard = new Shard(schemaId, getName(), getName());
		}
		QueriableRecordList recordList = new QueriableRecordListImpl(shard);
		shardRecordListMap.put(shard, recordList);
	}
	
	public void createRecordList(Shard shard, BlockPtr head) {
		QueriableRecordList recordList = new QueriableRecordListImpl(shard, head);
		shardRecordListMap.put(shard, recordList);
	}
	
	public QueriableRecordList getRecordList(Shard shard) {
		
		return shardRecordListMap.get(shard);
	}	
	
	public boolean isLoggingEnabled() {
		return isLoggingEnabled;
	}
	
	public List<Shard> getShards() {
		return new ArrayList<Shard>(shardRecordListMap.keySet());
	}
	
	@Override
	public byte getFileId(Shard shard) {
		return FileBlockManager.getInstance().getId(shard);
	}
	
	public int getBlockSize() {
		List<QueriableRecordList> list = new ArrayList<QueriableRecordList>(shardRecordListMap.values());
		Shard shard = list.get(0).getShard();
		return FileBlockManager.getInstance().getEntry(shard).getBlockSize();
	}
}
