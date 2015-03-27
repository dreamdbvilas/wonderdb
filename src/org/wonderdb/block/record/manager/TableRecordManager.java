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
package org.wonderdb.block.record.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexCompareIndexQuery;
import org.wonderdb.block.ListBlock;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.collection.TableResultContent;
import org.wonderdb.collection.exceptions.UniqueKeyViolationException;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.parser.UpdateSetExpression;
import org.wonderdb.parser.jtree.QueryEvaluator;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.DBInsertQuery;
import org.wonderdb.query.plan.AndQueryExecutor;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.IndexRecordMetadata;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.RecordManager;
import org.wonderdb.types.record.RecordManager.BlockAndRecord;
import org.wonderdb.types.record.TableRecord;


public class TableRecordManager {
	private static TableRecordManager instance = new TableRecordManager();
	private TableRecordManager() {
	}
	
	public static TableRecordManager getInstance() {
		return instance;
	}
	
	public synchronized Map<Integer, DBType> convertTypes(String collectionName, Map<String, DBType> ctMap) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		Map<Integer, DBType> retMap = new HashMap<Integer, DBType>();
		
		List<ColumnNameMeta> newColumns = new ArrayList<ColumnNameMeta>();
		
		Iterator<String> iter = ctMap.keySet().iterator();
		while (iter.hasNext()) {
			String col = iter.next();
			Integer id = colMeta.getColumnId(col);
			if (id == null || id < 0) {
				ColumnNameMeta cnm = new ColumnNameMeta();
				cnm.setCollectioName(collectionName);
				cnm.setColumnName(col);
				cnm.setColumnType(SerializerManager.STRING);
				newColumns.add(cnm);
			}
			int type = colMeta.getColumnType(col);
			StringType st = (StringType) ctMap.get(col);
			DBType dt = SerializerManager.getInstance().getSerializer(type).convert(type, st);
			retMap.put(id, dt);
		}
		
		if (newColumns.size() > 0) {
			colMeta.addColumns(newColumns);
		}
		
		return retMap;
	}

	public int addTableRecord(String tableName, Map<Integer, DBType> map, Shard shard, boolean insertOrUpdate) throws InvalidCollectionNameException {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		ViolatedKeysAndRecord ret = null;
		if (colMeta == null) {
			throw new RuntimeException("collection doesnt exist");
//			SchemaMetadata.getInstance().createNewCollection(tableName, null, null, 10);
//			colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		}
		
		Map<Integer, DBType> columnMap = new HashMap<Integer, DBType>(map);
		TableRecord trt = new TableRecord(columnMap);
		WonderDBList dbList = colMeta.getRecordList(shard);
		TransactionId txnId = null;
		if (colMeta.isLoggingEnabled()) {
			txnId = LogManager.getInstance().startTxn();
		}
		
		
		Set<Object> pinnedBlocks = new HashSet<Object>();
		RecordId recordId = null;
		Set<IndexKeyType> uIndexLock = null;
		try {
			List<IndexNameMeta> idxMetaList = SchemaMetadata.getInstance().getIndexes(tableName);
			ret = checkForUniqueViolations(idxMetaList, tableName, map, shard, null, pinnedBlocks);
			if (ret.anotherInflightTxn && insertOrUpdate) {
				return 0;
			}
			if (ret.recordId != null) {
				if (insertOrUpdate) {
					TableRecordMetadata meta = (TableRecordMetadata) SchemaMetadata.getInstance().getTypeMetadata(tableName);
					BlockAndRecord bar = null;
					bar = RecordManager.getInstance().getTableRecordAndLock(ret.recordId, new ArrayList<Integer>(map.keySet()), meta, pinnedBlocks);
					if (bar != null && bar.block != null && bar.record != null) {
						recordId = ((TableRecord) bar.record).getRecordId();
						ObjectLocker.getInstance().acquireLock(recordId);
						updateTableRecord(tableName, (TableRecord) bar.record, new ArrayList<Integer>(map.keySet()), new ArrayList<IndexNameMeta>(), columnMap, dbList, txnId, pinnedBlocks);
						return 1;
					} else {
						return 0;
					}
				} else {
					throw new UniqueKeyViolationException();
				}
			}
			TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(tableName);
			TableRecord savedRecord = (TableRecord) dbList.add(trt, txnId, meta, pinnedBlocks);
			
			recordId = savedRecord.getRecordId();
			ObjectLocker.getInstance().acquireLock(recordId);
			
			if (idxMetaList != null && idxMetaList.size() != 0) {
				for (int i = 0; i < idxMetaList.size(); i++) {
					IndexNameMeta idxMeta = idxMetaList.get(i);
					Shard idxShard = new Shard("");
					BTree tree = idxMeta.getIndexTree(idxShard);
					IndexKeyType ikt = buildIndexKey(idxMeta, savedRecord.getColumnMap(), savedRecord.getRecordId(), pinnedBlocks);
					try {
						tree.insert(ikt, pinnedBlocks, txnId);
					} catch (UniqueKeyViolationException e) {
						Logger.getLogger(getClass()).fatal("issue: got fatal error exception when not expected");
						throw e;
					}
				}
			}
			
//			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) && WonderDBPropertyManager.getInstance().isReplicationEnabled()) {
//				Producer<String, KafkaPayload> producer = KafkaProducerManager.getInstance().getProducer(shard.getReplicaSetName());
//				if (producer != null) {
//					String objectId = ((StringType) map.get(colMeta.getColumnType("objectId"))).get();
//					KafkaPayload payload = new KafkaPayload("insert", tableName, objectId, map);
//					KeyedMessage<String, KafkaPayload> message = new KeyedMessage<String, KafkaPayload>(shard.getReplicaSetName(), payload);
//					producer.send(message);
//				}
//			}
		} finally {
			if (ret != null && ret.indexSet != null) {
				InProcessIndexQueryIndexMgr.getInstance().done(ret.indexSet);
			}
			LogManager.getInstance().commitTxn(txnId);
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			InProcessIndexQueryIndexMgr.getInstance().done(uIndexLock);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return 1;
	}
	
	private class ViolatedKeysAndRecord {
		Set<IndexKeyType> indexSet = null;
		RecordId recordId = null;
		boolean anotherInflightTxn = false;
	}

	private ViolatedKeysAndRecord checkForUniqueViolations(List<IndexNameMeta> idxMetaList, String tableName, Map<Integer, DBType> map, 
			Shard tableShard, RecordId recordId, Set<Object> pinnedBlocks) {
		Set<IndexKeyType> uniqueIndexKeys = new HashSet<IndexKeyType>();
		
		if (idxMetaList != null && idxMetaList.size() != 0) {
			for (int i = 0; i < idxMetaList.size(); i++) {
				IndexNameMeta idxMeta = idxMetaList.get(i);
				if (idxMeta.isUnique()) {
					IndexKeyType ikt = buildIndexKey(idxMeta, map, null, pinnedBlocks);
					uniqueIndexKeys.add(ikt);
				}
			}
		}
		
		ViolatedKeysAndRecord ret = new ViolatedKeysAndRecord();
		ret.indexSet = uniqueIndexKeys;
		
		if (InProcessIndexQueryIndexMgr.getInstance().canProcess(uniqueIndexKeys)) {
			for (int i = 0; i < idxMetaList.size(); i++) {
				IndexNameMeta idxMeta = idxMetaList.get(i);
				TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(idxMeta);
				if (idxMeta.isUnique()) {
					IndexKeyType ikt = buildIndexKey(idxMeta, map, null, pinnedBlocks);
					Shard shard = new Shard("");
					BTree tree = idxMeta.getIndexTree(shard);
					IndexCompareIndexQuery iciq = new IndexCompareIndexQuery(ikt, true, meta, pinnedBlocks);
					ResultIterator iter = null;
					try {
						iter = tree.find(iciq, false, pinnedBlocks);
						if (iter.hasNext()) {
							IndexRecord record = (IndexRecord) iter.next();
							DBType column = record.getColumn();
							IndexKeyType ikt1 = (IndexKeyType) column;
							RecordId ikt1RecId = ikt1.getRecordId();
							if (ikt.compareTo(ikt1) == 0 && !ikt1RecId.equals(recordId)) {
								ret.recordId = ikt1RecId;
								return ret;
//								InProcessIndexQueryIndexMgr.getInstance().done(uniqueIndexKeys);
//								return null;
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (iter != null) {
							iter.unlock();
						}
					}
				}
			}
		} else {
			ret.anotherInflightTxn = true;
		}
		return ret;
	}
	
	public int updateTableRecord(String tableName, RecordId recordId, Shard shard, List<Integer> selectColumns, 
			List<UpdateSetExpression> updateSetList, SimpleNode tree, Map<String, CollectionAlias> fromMap, TransactionId txnId) throws InvalidCollectionNameException {
		// first lets delete all indexes with old value.
//		Map<Integer, Column> columnMap = record.getColumnMap();
//		Map<Integer, DBType> ctMap = new HashMap<>(columnMap.size());
		
//		Iterator<Integer> iter = columnMap.keySet().iterator();
//		while (iter.hasNext()) {
//			int key = iter.next();
//			Column column = columnMap.get(key);
//			ctMap.put(key, column.getValue());
//		}
		TableRecord record = null;
		Set<IndexKeyType> uIndexLock = null;
		Set<Object> pinnedBlocks = new HashSet<Object>();
		ViolatedKeysAndRecord ret = null;
		CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
		CollectionAlias ca = new CollectionAlias(tableName, "");
		DataContext context = new DataContext();
		ObjectLocker.getInstance().acquireLock(recordId);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		if (colMeta.isLoggingEnabled()) {
			txnId = LogManager.getInstance().startTxn();
		}
		WonderDBList dbList = colMeta.getRecordList(shard);
		TableRecordMetadata meta = (TableRecordMetadata) SchemaMetadata.getInstance().getTypeMetadata(tableName);
		try {
//			ListBlock rb = (ListBlock) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, pinnedBlocks);
//			if (rb == null) {
//				return 0;
//			}
//			rb.readLock();
//			try {
//				record = (TableRecord) rb.getRecord(recordId.getPosn());
//				if (record == null) {
//					return 0;
//				}
//				context.add(ca, new TableResultContent(record, meta, pinnedBlocks));
//				if (!AndQueryExecutor.filterTree(context, tree, fromMap)) {
//					return 0;
//				}
//			} finally {
//				if (rb != null) {
//					rb.readUnlock();
//				}
//			}
			
			BlockAndRecord bar = null;
			bar = RecordManager.getInstance().getTableRecordAndLock(recordId, selectColumns, meta, pinnedBlocks);
			if (bar == null || bar.block == null || bar.record == null) {
				return 0;
			}
			record = (TableRecord) bar.record;
			context.add(ca, new TableResultContent(record, meta));
			if (!AndQueryExecutor.filterTree(context, tree, fromMap)) {
				return 0;
			}
			
			Map<Integer, DBType> changedValues = new HashMap<Integer, DBType>();
			updateChangedValues(context, updateSetList, changedValues, fromMap);
			List<IndexNameMeta> changedIndexes = getChangedIndexes(tableName, changedValues);

			Map<Integer, DBType> columnMap = record.getColumnMap();
			Map<Integer, DBType> ctMap = new HashMap<Integer, DBType>(columnMap.size());
			
			Iterator<Integer> iter = columnMap.keySet().iterator();
			while (iter.hasNext()) {
				int key = iter.next();
				DBType column = columnMap.get(key);
				ctMap.put(key, column);
			}
			Map<Integer, DBType> workingCtMap = new HashMap<Integer, DBType>(ctMap);
			workingCtMap.putAll(changedValues);
			
			ret = checkForUniqueViolations(changedIndexes, tableName, workingCtMap, shard, recordId, pinnedBlocks);
			if (ret.recordId != null) {
				return 0;
			}
			updateTableRecord(tableName, record, selectColumns, changedIndexes, changedValues, dbList, txnId, pinnedBlocks);
		} finally {
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			if (ret != null && ret.indexSet != null) {
				InProcessIndexQueryIndexMgr.getInstance().done(ret.indexSet);
			}
			InProcessIndexQueryIndexMgr.getInstance().done(uIndexLock);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return 1;
	}

	private void updateTableRecord(String tableName, TableRecord record, List<Integer> selectColumns, List<IndexNameMeta> changedIndexes, 
			Map<Integer, DBType> changedValues, WonderDBList dbList, TransactionId txnId, Set<Object> pinnedBlocks) {
		
		
		for (int i = 0; i < changedIndexes.size(); i++) {
			IndexNameMeta idxMeta = changedIndexes.get(i);
			IndexKeyType ikt = buildIndexKey(idxMeta, record.getColumnMap(), record.getRecordId(), pinnedBlocks);
			Shard idxShard = new Shard("");
			idxMeta.getIndexTree(idxShard).remove(ikt, pinnedBlocks, txnId);
		}
		
		TableRecord newRecord = (TableRecord) record.copyOf();
		Iterator<Integer> iter1 = changedValues.keySet().iterator();
		
		while (iter1.hasNext()) {
			int key = iter1.next();
			DBType newValue = changedValues.get(key);
			DBType currentValue = newRecord.getColumnMap().get(key);
			if (currentValue instanceof ExtendedColumn) {
				((ExtendedColumn) currentValue).setValue(newValue);
			} else {
				newRecord.getColumnMap().put(key, newValue);
			}
		}
		
		TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(tableName);
		dbList.update(record, newRecord, txnId, meta, pinnedBlocks);
		if (changedIndexes != null) {
			for (int i = 0; i < changedIndexes.size(); i++) {
				IndexNameMeta idxMeta = changedIndexes.get(i);
				IndexKeyType ikt = buildIndexKey(idxMeta, newRecord.getColumnMap(), newRecord.getRecordId(), pinnedBlocks);
				try {
					Shard idxShard = new Shard("");
					IndexNameMeta inm = SchemaMetadata.getInstance().getIndex(idxMeta.getIndexName());
					inm.getIndexTree(idxShard).insert(ikt, pinnedBlocks, txnId);
				} catch (UniqueKeyViolationException e) {
					throw new RuntimeException("Unique Key violation: should never happen");
				}
			}
		}		
	}
	
	private void updateChangedValues(DataContext context, List<UpdateSetExpression> updateSetList, Map<Integer, DBType> changedValue, Map<String, CollectionAlias> fromMap) {
		for (int i = 0; i < updateSetList.size(); i++) {
			UpdateSetExpression use = updateSetList.get(i);
			QueryEvaluator qEval = new QueryEvaluator(fromMap, context);
			qEval.processMultiplicativeExpression(use.value);
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(use.column.getCollectionAlias().getCollectionName());
			String colName = colMeta.getColumnName(use.column.getColumnId());
			int type = colMeta.getColumnType(colName);
			
			DBType dt = DBInsertQuery.convertToDBType(use.value.jjtGetValue(), type);
			changedValue.put(use.column.getColumnId(), dt);
		}
	}
	
	public int delete(String tableName, Shard shard, RecordId recordId, SimpleNode filterNode, Map<String, CollectionAlias> fromMap) {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		TableRecord record = null;
		TransactionId txnId = null;

		try {
			ObjectLocker.getInstance().acquireLock(recordId);
			DataContext context = new DataContext();
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
			if (colMeta.isLoggingEnabled()) {
				txnId = LogManager.getInstance().startTxn();
			}

			TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(tableName);
			ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, pinnedBlocks);
			if (block == null) {
				return 0;
			}
			block.readLock();
			record = (TableRecord) block.getRecord(recordId.getPosn());
			StaticTableResultContent content = new StaticTableResultContent(record);
			CollectionAlias ca = new CollectionAlias(tableName, "");
			context.add(new CollectionAlias(tableName, ""), content);
			try {
				if (!AndQueryExecutor.filterTree(context, filterNode, fromMap)) {
					return 0;
				}				
			} finally {
				block.readUnlock();
			}
			
			List<IndexNameMeta> idxMetaList = SchemaMetadata.getInstance().getIndexes(tableName);
			// now just walk through the results and remove it from all indexes.
			deleteRecordFromIndexes(context, idxMetaList, shard, ca, pinnedBlocks, txnId);			
			WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata(tableName).getRecordList(shard);
			dbList.deleteRecord(record.getRecordId(), txnId, meta, pinnedBlocks);
			
//			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) && WonderDBPropertyManager.getInstance().isReplicationEnabled()) {
//				Producer<String, KafkaPayload> producer = KafkaProducerManager.getInstance().getProducer(shard.getReplicaSetName());
//				if (producer != null) {
//					String objectId = ((StringType) tr.getColumnValue(colMeta, colMeta.getColumnType("objectId"), null)).get();
//					KafkaPayload payload = new KafkaPayload("delete", tableName, objectId, null);
//					KeyedMessage<String, KafkaPayload> message = new KeyedMessage<String, KafkaPayload>(shard.getReplicaSetName(), payload);
//					producer.send(message);
//				}
//			}

		} finally {
			LogManager.getInstance().commitTxn(txnId);
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		
		return 1;
	}
	
	public void deleteFromCache(DBType key) {
		if (key == null) {
			return;
		}
		ObjectLocker.getInstance().acquireLock(key);
		try {
			List<IndexNameMeta> idxMetaList = SchemaMetadata.getInstance().getIndexes("cache");
			IndexNameMeta inm = idxMetaList.get(0);
			Shard shard = new Shard("");
			Set<Object> pinnedBlocks = new HashSet<Object>();
			TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(inm);
			List<DBType> list = new ArrayList<DBType>();
			list.add(key);
			IndexKeyType ikt = new IndexKeyType(list, null);
			TransactionId txnId = LogManager.getInstance().startTxn();
			BTree tree = inm.getIndexTree(shard);
			IndexCompareIndexQuery iciq = new IndexCompareIndexQuery(ikt, true, meta, pinnedBlocks);
			
			ResultIterator iter = tree.find(iciq, true, pinnedBlocks);
			while (iter.hasNext()) {
				IndexRecord record = (IndexRecord) iter.next();
				iter.remove();
				int i = 0;
			}
			ikt = tree.remove(ikt, pinnedBlocks, txnId);
			WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata("cache").getRecordList(shard);
			if (ikt != null) {
				dbList.deleteRecord(ikt.getRecordId(), txnId, meta, pinnedBlocks);
			}
		} finally {
			ObjectLocker.getInstance().releaseLock(key);
		}
		
	}
	
//	public int delete (String collectionName, String objectId, Shard shard) {
//		RecordId recId = getRecordIdFromObjectId(collectionName, objectId, shard);
//		return delete(collectionName, shard, recId, null);
//	}
//	
	private void deleteRecordFromIndexes(DataContext context, List<IndexNameMeta> idxMetaList, Shard tableShard, CollectionAlias ca, 
			Set<Object> pinnedBlocks, TransactionId txnId) {
		for (int j = 0; j < idxMetaList.size(); j++) {
			IndexNameMeta idxMeta = idxMetaList.get(j);
			IndexKeyType ikt = buildIndexKeyForDelete(idxMeta, context, ca, pinnedBlocks);
			Shard shard = new Shard("");
			IndexNameMeta inm = SchemaMetadata.getInstance().getIndex(idxMeta.getIndexName());
			inm.getIndexTree(shard).remove(ikt, pinnedBlocks, txnId);
		}		
	}
	
//	public void upsertRecord(String tableName, Map<String, DBType> changedColumns) throws InvalidCollectionNameException {
//		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
//		Map<ColumnType, DBType> ctMap = convertTypes(tableName, changedColumns);
//		List<IndexMetadata> idxMetaList = SchemaMetadata.getInstance().getIndexes(tableName);
//		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
//		
//		RecordAndRecrdBlock rarb = null;
//		for (int i = 0; i < idxMetaList.size(); i++) {
//			IndexMetadata idxMeta = idxMetaList.get(i);
//			if (idxMeta.getIndex().isUnique()) {
//				rarb = getRecord(idxMeta, ctMap, true, pinnedBlocks); 
//				break;
//			}
//		}
//		
//		if (rarb != null) {
//			updateTableRecord(tableName, ctMap, rarb.record, rarb.recordBock, pinnedBlocks);
//		} else {
//			addTableRecord(tableName, ctMap, pinnedBlocks);
//		}		
//	}
//	
	private List<IndexNameMeta> getChangedIndexes(String tableName, Map<Integer, DBType> changedColumns) {
		List<IndexNameMeta> tableIndexes = SchemaMetadata.getInstance().getIndexes(tableName);
		List<IndexNameMeta> retList = new ArrayList<IndexNameMeta>();
		
		for (int i = 0; i < tableIndexes.size(); i++) {
			IndexNameMeta idxMeta = tableIndexes.get(i);
			List<Integer> idxSet = idxMeta.getColumnIdList();
			Iterator<Integer> iter = idxSet.iterator();
			boolean found = false;
			
			while (iter.hasNext()) {
				Integer c = iter.next();
				if (changedColumns.containsKey(c)) {
					found = true;
					break;
				} 
			}
			if (found) {
				retList.add(idxMeta);
			}
		}
		
		return retList;
	}

	private IndexKeyType buildIndexKey(IndexNameMeta idxMeta, Map<Integer, DBType> columnMap, RecordId recordId, Set<Object> pinnedBlocks) {
		List<Integer> idxColList = idxMeta.getColumnIdList();
		IndexRecordMetadata meta = (IndexRecordMetadata) SchemaMetadata.getInstance().getIndexMetadata(idxMeta);
		List<DBType> idxCols = new ArrayList<DBType>();
		for (int i = 0; i < idxColList.size(); i++) {
			int colId = idxColList.get(i);
			DBType column = columnMap.get(colId);
			DBType dt = column;
			if (column instanceof ExtendedColumn) {
				int type = meta.getTypeList().get(i);
				dt = ((ExtendedColumn)  column).getValue(new ColumnSerializerMetadata(type));
			}
			idxCols.add(dt);
		}
		
		return new IndexKeyType(idxCols, recordId);
	}
	
//	private IndexKeyType buildIndexKeyForUpdate(IndexNameMeta idxMeta, TableRecord record, Set<Object> pinnedBlocks) {
//		List<Integer> idxColList = idxMeta.getColumnIdList();
//		List<DBType> idxCols = new ArrayList<>();
//		IndexRecordMetadata meta = (IndexRecordMetadata) SchemaMetadata.getInstance().getIndexMetadata(idxMeta);
//		
//		for (int i = 0; i < idxColList.size(); i++) {
//			Integer colId = idxColList.get(i);
//			DBType column = record.getColumnMap().get(colId);
//			
//			if (column instanceof ExtendedColumn) {
//				int type = meta.getTypeList().get(i);
//				column = ((ExtendedColumn) column).getValue(new ColumnSerializerMetadata(type), pinnedBlocks);
//			}
//			idxCols.add(column);
//		}
//		return new IndexKeyType(idxCols, record.getRecordId());
//	}
	
//	private IndexKeyType buildIndexKeyForUpdate(IndexMetadata idxMeta, DataContext context, CollectionAlias ca, Set<BlockPtr> pinnedBlocks) {
//		List<CollectionColumn> idxColList = idxMeta.getIndex().getColumnList();
//		List<DBType> idxCols = new ArrayList<DBType>();
//		
//		for (int i = 0; i < idxColList.size(); i++) {
//			ColumnType colType = idxColList.get(i).getColumnType();
//			idxCols.add(context.getValue(ca, colType, null));
//		}
//		
//		return new IndexKeyType(idxCols, context.get(ca));
//	}
	
	private IndexKeyType buildIndexKeyForDelete(IndexNameMeta idxMeta, DataContext context, CollectionAlias ca, Set<Object> pinnedBlocks) {
		List<Integer> idxColList = idxMeta.getColumnIdList();
		List<DBType> idxCols = new ArrayList<DBType>();
		
		for (int i = 0; i < idxColList.size(); i++) {
			Integer colType = idxColList.get(i);
			idxCols.add(context.getValue(ca, colType, null));
		}
		
		return new IndexKeyType(idxCols, context.get(ca));
	}
	
//	private IndexKeyType buildIndexKeyForSelect(IndexNameMeta idxMeta, Map<Integer, DBType> columns, Set<Object> pinnedBlocks) {
//		List<Integer> idxColList = idxMeta.getColumnIdList();
//		List<DBType> idxCols = new ArrayList<DBType>();
//		IndexRecordMetadata meta = (IndexRecordMetadata) SchemaMetadata.getInstance().getIndexMetadata(idxMeta);
//		
//		for (int i = 0; i < idxColList.size(); i++) {
//			int colId = idxColList.get(i);
//			DBType column = columns.get(colId);
//			if (column instanceof ExtendedColumn) {
//				int type = meta.getTypeList().get(colId);
//				column = ((ExtendedColumn) column).getValue(new ColumnSerializerMetadata(type), pinnedBlocks);
//			}
//			idxCols.add(column);
//		}
//		
//		return new IndexKeyType(idxCols, null);
//	}
}
