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
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
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
		
		List<ColumnNameMeta> newColumns = new ArrayList<>();
		
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

	public int addTableRecord(String tableName, Map<Integer, DBType> map, Shard shard) throws InvalidCollectionNameException {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);

		if (colMeta == null) {
			throw new RuntimeException("collection doesnt exist");
//			SchemaMetadata.getInstance().createNewCollection(tableName, null, null, 10);
//			colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		}
		
		TransactionId txnId = null;
		if (colMeta.isLoggingEnabled()) {
			txnId = LogManager.getInstance().startTxn();
		}
		
		Map<Integer, DBType> columnMap = new HashMap<>(map);
		
		TableRecord trt = new TableRecord(columnMap);
		Set<Object> pinnedBlocks = new HashSet<Object>();
		RecordId recordId = null;
		Set<IndexKeyType> uIndexLock = null;
		try {
			List<IndexNameMeta> idxMetaList = SchemaMetadata.getInstance().getIndexes(tableName);
			uIndexLock = checkForUniqueViolations(idxMetaList, tableName, map, shard, null, pinnedBlocks);
			if (uIndexLock == null) {
				throw new UniqueKeyViolationException();
			}
			TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(tableName);
			WonderDBList dbList = colMeta.getRecordList(shard);
			TableRecord savedRecord = (TableRecord) dbList.add(trt, txnId, meta, pinnedBlocks);
			recordId = savedRecord.getRecordId();
			ObjectLocker.getInstance().acquireLock(recordId);
			
			if (idxMetaList != null && idxMetaList.size() != 0) {
				for (int i = 0; i < idxMetaList.size(); i++) {
					IndexNameMeta idxMeta = idxMetaList.get(i);
					Shard idxShard = new Shard("");
					BTree tree = idxMeta.getIndexTree(idxShard);
					IndexKeyType ikt = buildIndexKey(idxMeta, savedRecord);
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
			LogManager.getInstance().commitTxn(txnId);
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			InProcessIndexQueryIndexMgr.getInstance().done(uIndexLock);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return 1;
	}

	private Set<IndexKeyType> checkForUniqueViolations(List<IndexNameMeta> idxMetaList, String tableName, Map<Integer, DBType> map, 
			Shard tableShard, RecordId recordId, Set<Object> pinnedBlocks) {
		Set<IndexKeyType> uniqueIndexKeys = new HashSet<IndexKeyType>();
		
		if (idxMetaList != null && idxMetaList.size() != 0) {
			for (int i = 0; i < idxMetaList.size(); i++) {
				IndexNameMeta idxMeta = idxMetaList.get(i);
				if (idxMeta.isUnique()) {
					IndexKeyType ikt = buildIndexKeyForSelect(idxMeta, map);
					uniqueIndexKeys.add(ikt);
				}
			}
		}
		
		if (InProcessIndexQueryIndexMgr.getInstance().canProcess(uniqueIndexKeys)) {
			for (int i = 0; i < idxMetaList.size(); i++) {
				IndexNameMeta idxMeta = idxMetaList.get(i);
				TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(idxMeta);
				if (idxMeta.isUnique()) {
					IndexKeyType ikt = buildIndexKeyForSelect(idxMeta, map);
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
								InProcessIndexQueryIndexMgr.getInstance().done(uniqueIndexKeys);
								return null;
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
		}
		return uniqueIndexKeys;
	}
	
//	public int updateTableRecord(String tableName, Map<String, DBType> map, Shard shard, RecordId recordId, List<BasicExpression> expList,
//			TransactionId txnId) throws InvalidCollectionNameException {
//		Set<Object> pinnedBlocks = new HashSet<Object>();
//		try {
//			Map<Integer, DBType> ctMap = convertTypes(tableName, map);
//			return updateTableRecord(tableName, ctMap, shard, recordId, expList, pinnedBlocks, txnId);
//		} finally {
//			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
//		}
//	}
//	
//	public int updateTableRecord(String tableName, String objectId, Map<Integer, DBType> map, Shard shard) {
//		RecordId recId = getRecordIdFromObjectId(tableName, objectId, shard);
//		Set<Object> pinnedBlocks = new HashSet<>();
//		try {
//			return updateTableRecord(tableName, map, shard, recId, null, pinnedBlocks, null);
//		} catch (InvalidCollectionNameException e) {
//			throw new RuntimeException(e);
//		} finally {
//			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
//		}
//	}
//	
//	private RecordId getRecordIdFromObjectId(String tableName, String objectId, Shard shard) {
//		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex("objectId"+tableName);
//		Shard idxShard = new Shard(idxMeta.getSchemaId(), idxMeta.getName(), shard.getReplicaSetName());
//		BTree tree = idxMeta.getIndexTree(idxShard);
//		List<DBType> l = new ArrayList<DBType>();
//		l.add(new StringType(objectId));
//		IndexKeyType ikt = new IndexKeyType(l, null);
//		IndexCompareIndexQuery entry = new IndexCompareIndexQuery(ikt);
//		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
//		ResultIterator iter = null;
//		RecordId recId = null;
//		try {
//			iter = tree.find(entry, false, pinnedBlocks);
//			if (iter.hasNext()) {
//				ResultContent rc = iter.next();
//				recId = rc.getRecordId();
//			}
//		} finally {
//			iter.unlock();
//			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
//		}
//		return recId;
//	}
//	private List<ColumnType> buildSelectCoulmnList(String collectionName, Map<ColumnType, DBType> changedColumns, List<BasicExpression> expList) {
//		Set<ColumnType> selectList = new HashSet<ColumnType>();
//		selectList.addAll(changedColumns.keySet());
//		Iterator<DBType> values = changedColumns.values().iterator();
//		while (values.hasNext()) {
//			DBType dt = values.next();
//			if (dt instanceof CollectionColumn) {
//				selectList.add(((CollectionColumn) dt).getColumnType());
//			}
//		}
//
//		if (expList != null) {		
//			for (int i = 0; i < expList.size(); i++) {
//				BasicExpression exp = expList.get(i);
//				if (exp.getLeftOperand() instanceof VariableOperand) {
//					VariableOperand vo = (VariableOperand) exp.getLeftOperand();
//					if (vo.getCollectionAlias() != null && vo.getCollectionAlias().getCollectionName().equals(collectionName)) {
//						selectList.add(vo.getColumnType());
//					}
//				}
//			}
//		}
//		
//		List<IndexMetadata> idxList = SchemaMetadata.getInstance().getIndexes(collectionName);
//		for (int i = 0; i < idxList.size(); i++) {
//			IndexMetadata idxMeta = idxList.get(i);
//			selectList.addAll(idxMeta.getIndex().getColumnNameList());
//		}
//		return new ArrayList<ColumnType>(selectList);
//	}
//	
	public int updateTableRecord(String tableName, RecordId recordId, Shard shard, List<UpdateSetExpression> updateSetList, 
			SimpleNode tree, Map<String, CollectionAlias> fromMap, TransactionId txnId) throws InvalidCollectionNameException {
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
		Set<Object> pinnedBlocks = new HashSet<>();
		
		CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
		CollectionAlias ca = new CollectionAlias(tableName, "");
		DataContext context = new DataContext();
		ObjectLocker.getInstance().acquireLock(recordId);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		if (colMeta.isLoggingEnabled()) {
			txnId = LogManager.getInstance().startTxn();
		}
		WonderDBList dbList = colMeta.getRecordList(shard);
		TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(tableName);
		try {
			ListBlock rb = (ListBlock) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, pinnedBlocks);
			if (rb == null) {
				return 0;
			}
			rb.readLock();
			try {
				record = (TableRecord) rb.getRecord(recordId.getPosn());
				if (record == null) {
					return 0;
				}
				context.add(ca, new TableResultContent(record, meta, pinnedBlocks));
				if (!AndQueryExecutor.filterTree(context, tree, fromMap)) {
					return 0;
				}
			} finally {
				if (rb != null) {
					rb.readUnlock();
				}
			}
			Map<Integer, DBType> changedValues = new HashMap<>();
			updateChangedValues(context, updateSetList, changedValues, fromMap);
			List<IndexNameMeta> changedIndexes = getChangedIndexes(tableName, changedValues);

			Map<Integer, DBType> columnMap = record.getColumnMap();
			Map<Integer, DBType> ctMap = new HashMap<>(columnMap.size());
			
			Iterator<Integer> iter = columnMap.keySet().iterator();
			while (iter.hasNext()) {
				int key = iter.next();
				DBType column = columnMap.get(key);
				if (column == null) {
					int breakHere = 0;
				}
				ctMap.put(key, column);
			}
			Map<Integer, DBType> workingCtMap = new HashMap<>(ctMap);
			workingCtMap.putAll(changedValues);
			
			uIndexLock = checkForUniqueViolations(changedIndexes, tableName, workingCtMap, shard, recordId, pinnedBlocks);
			if (uIndexLock == null) {
				return 0;
			}
						
			for (int i = 0; i < changedIndexes.size(); i++) {
				IndexNameMeta idxMeta = changedIndexes.get(i);
				IndexKeyType ikt = buildIndexKeyForUpdate(idxMeta, record, pinnedBlocks);
				Shard idxShard = new Shard("");
				idxMeta.getIndexTree(idxShard).remove(ikt, pinnedBlocks, txnId);
			}
			
			TableRecord newRecord = new TableRecord(changedValues);
			Iterator<Integer> iter1 = record.getColumnMap().keySet().iterator();
			
			while (iter1.hasNext()) {
				int key = iter1.next();
				if (changedValues.containsKey(key)) {
					continue;
				}
				DBType value = record.getColumnMap().get(key);
				changedValues.put(key, value);
			}

			dbList.update(record, newRecord, txnId, meta, pinnedBlocks);
			for (int i = 0; i < changedIndexes.size(); i++) {
				IndexNameMeta idxMeta = changedIndexes.get(i);
				IndexKeyType ikt = buildIndexKeyForUpdate(idxMeta, record, pinnedBlocks);
				try {
					Shard idxShard = new Shard("");
					IndexNameMeta inm = SchemaMetadata.getInstance().getIndex(idxMeta.getIndexName());
					inm.getIndexTree(idxShard).insert(ikt, pinnedBlocks, txnId);
				} catch (UniqueKeyViolationException e) {
					throw new RuntimeException("Unique Key violation: should never happen");
				}
			}
			
//			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) && WonderDBPropertyManager.getInstance().isReplicationEnabled()) {
//				Producer<String, KafkaPayload> producer = KafkaProducerManager.getInstance().getProducer(shard.getReplicaSetName());
//				if (producer != null) {
//					DBType dt = tr.getColumnValue(colMeta, colMeta.getColumnType("objectId"), null);
//					String objectId = ((StringType) dt).get();
//					KafkaPayload payload = new KafkaPayload("update", tableName, objectId, ctMap);
//					KeyedMessage<String, KafkaPayload> message = new KeyedMessage<String, KafkaPayload>(shard.getReplicaSetName(), payload);
//					producer.send(message);
//				}
//			}
		} finally {
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			InProcessIndexQueryIndexMgr.getInstance().done(uIndexLock);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return 1;
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
			dbList.deleteRecord(record, txnId, meta, pinnedBlocks);
			
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

	private IndexKeyType buildIndexKey(IndexNameMeta idxMeta, TableRecord record) {
		List<Integer> idxColList = idxMeta.getColumnIdList();
		List<DBType> idxCols = new ArrayList<>();
		for (int i = 0; i < idxColList.size(); i++) {
			int colId = idxColList.get(i);
			DBType column = record.getColumnMap().get(colId);
			DBType dt = column;
			idxCols.add(dt);
		}
		
		return new IndexKeyType(idxCols, record.getRecordId());
	}
	
	private IndexKeyType buildIndexKeyForUpdate(IndexNameMeta idxMeta, TableRecord record, Set<Object> pinnedBlocks) {
		List<Integer> idxColList = idxMeta.getColumnIdList();
		List<DBType> idxCols = new ArrayList<>();
		
		for (int i = 0; i < idxColList.size(); i++) {
			Integer colId = idxColList.get(i);
			DBType column = record.getColumnMap().get(colId);
			idxCols.add(column);
		}
		return new IndexKeyType(idxCols, record.getRecordId());
	}
	
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
	
	private IndexKeyType buildIndexKeyForSelect(IndexNameMeta idxMeta, Map<Integer, DBType> columns) {
		List<Integer> idxColList = idxMeta.getColumnIdList();
		List<DBType> idxCols = new ArrayList<DBType>();
		
		for (int i = 0; i < idxColList.size(); i++) {
			int colId = idxColList.get(i);
			idxCols.add(columns.get(colId));
		}
		
		return new IndexKeyType(idxCols, null);
	}
}
