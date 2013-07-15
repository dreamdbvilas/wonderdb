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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.log4j.Logger;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.impl.base.IndexCompareIndexQuery;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.cluster.kafka.KafkaPayload;
import org.wonderdb.cluster.kafka.KafkaProducerManager;
import org.wonderdb.collection.BTree;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.collection.TableResultContent;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.collection.exceptions.UniqueKeyViolationException;
import org.wonderdb.collection.impl.QueriableRecordList;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.query.plan.AndQueryExecutor;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.CollectionColumnSerializer;
import org.wonderdb.seralizers.SerializerManager;
import org.wonderdb.seralizers.StringSerializer;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;
import org.wonderdb.types.impl.StringType;


public class TableRecordManager {
	private static TableRecordManager instance = new TableRecordManager();
	private TableRecordManager() {
	}
	
	public static TableRecordManager getInstance() {
		return instance;
	}
	
	public synchronized Map<ColumnType, DBType> convertTypes(String collectionName, Map<String, DBType> ctMap) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		Map<ColumnType, DBType> retMap = new HashMap<ColumnType, DBType>();
		
		CollectionColumnSerializer<? extends SerializableType> s = StringSerializer.getInstance();
		Map<ColumnType, DBType> newColumns = new HashMap<ColumnType, DBType>();
		
		Iterator<String> iter = ctMap.keySet().iterator();
		while (iter.hasNext()) {
			String col = iter.next();
			Integer id = colMeta.getColumnId(col);
			ColumnType ct = null;
			if (id == null || id < 0) {
				CollectionColumn cc = colMeta.add(col);
				id = colMeta.getColumnId(col);
				ct = new ColumnType(id+3);
				newColumns.put(ct, cc);
			}
			ct = new ColumnType(id);
			s = SerializerManager.getInstance().getSerializer(colMeta.getColumnSerializerName(ct));
			if (s == null) {
				s = StringSerializer.getInstance();
			}
			DBType objVal = s.getDBObject(ctMap.get(col));
			retMap.put(ct, objVal);
		}
		
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		if (newColumns.size() > 0) {
			TransactionId txnId = LogManager.getInstance().startTxn();
			try {
				Shard shard = new Shard(1, "", "");
//				SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
				updateTableRecord("metaCollection", newColumns, shard, colMeta.getRecordId(), null, pinnedBlocks, txnId);
				List<CollectionColumn> l = new ArrayList<CollectionColumn>();
				Iterator<DBType> itr = newColumns.values().iterator();
				while(itr.hasNext()) {
					CollectionColumn cc = (CollectionColumn) itr.next();
					l.add(cc);
				}
				ClusterManagerFactory.getInstance().getClusterManager().addColumns(collectionName, l);
			} catch (InvalidCollectionNameException e) {
			} finally {
				LogManager.getInstance().commitTxn(txnId);
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
		
		return retMap;
	}

	public int addTableRecord(String tableName, Map<ColumnType, DBType> map, Shard shard) throws InvalidCollectionNameException {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		
		if (colMeta == null) {
			SchemaMetadata.getInstance().addCollection(tableName, null);
			colMeta = SchemaMetadata.getInstance().getCollectionMetadata(tableName);
		}
		
//		if (!map.containsKey("objectId")) {
//			ObjectId objectId = new ObjectId(ClusterManagerFactory.getInstance().getClusterManager().getMachineId());
//			StringType st = new StringType(objectId.toString());
//			map.put("objectId", st);
//		}
		
		TransactionId txnId = null;
//		if (colMeta.isLoggingEnabled()) {
			txnId = LogManager.getInstance().startTxn();
//		}
		
//		Map<ColumnType, DBType> ctMap = convertTypes(tableName, map);
		Map<ColumnType, DBType> ctMap = map;
		TableRecord trt = new TableRecord(ctMap);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		RecordId recordId = null;
		Set<IndexKeyType> uIndexLock = null;
		try {
			List<IndexMetadata> idxMetaList = SchemaMetadata.getInstance().getIndexes(tableName);
			uIndexLock = checkForUniqueViolations(idxMetaList, tableName, ctMap, shard, null, pinnedBlocks);
			if (uIndexLock == null) {
				throw new UniqueKeyViolationException();
			}
			QueriableRecordList trl = colMeta.getRecordList(shard);
			RecordBlock recordBlock = trl.add(trt, pinnedBlocks, txnId);
			recordId = trt.getRecordId();
			ObjectLocker.getInstance().acquireLock(recordId);
			
			if (idxMetaList != null && idxMetaList.size() != 0) {
				for (int i = 0; i < idxMetaList.size(); i++) {
					IndexMetadata idxMeta = idxMetaList.get(i);
					Shard idxShard = new Shard(idxMeta.getSchemaId(), idxMeta.getName(), shard.getReplicaSetName());
					BTree tree = idxMeta.getIndexTree(idxShard);
					IndexKeyType ikt = buildIndexKey(idxMeta, trt, recordBlock);
					try {
						tree.insert(ikt, pinnedBlocks, txnId);
					} catch (UniqueKeyViolationException e) {
						Logger.getLogger(getClass()).fatal("issue: got fatal error exception when not expected");
						throw e;
					}
				}
			}
			
			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) && WonderDBPropertyManager.getInstance().isReplicationEnabled()) {
				Producer<String, KafkaPayload> producer = KafkaProducerManager.getInstance().getProducer(shard.getReplicaSetName());
				if (producer != null) {
					String objectId = ((StringType) map.get(colMeta.getColumnType("objectId"))).get();
					KafkaPayload payload = new KafkaPayload("insert", tableName, objectId, map);
					KeyedMessage<String, KafkaPayload> message = new KeyedMessage<String, KafkaPayload>(shard.getReplicaSetName(), payload);
					producer.send(message);
				}
			}
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

	private Set<IndexKeyType> checkForUniqueViolations(List<IndexMetadata> idxMetaList, String tableName, Map<ColumnType, DBType> map, Shard tableShard, RecordId recordId, Set<BlockPtr> pinnedBlocks) {
		Set<IndexKeyType> uniqueIndexKeys = new HashSet<IndexKeyType>();
		TableRecord trt = new TableRecord(map);
		
		if (idxMetaList != null && idxMetaList.size() != 0) {
			for (int i = 0; i < idxMetaList.size(); i++) {
				IndexMetadata idxMeta = idxMetaList.get(i);
				if (idxMeta.getIndex().isUnique()) {
					IndexKeyType ikt = buildIndexKeyForSelect(idxMeta, trt);
					uniqueIndexKeys.add(ikt);
				}
			}
		}
		
		if (InProcessIndexQueryIndexMgr.getInstance().canProcess(uniqueIndexKeys)) {
			for (int i = 0; i < idxMetaList.size(); i++) {
				IndexMetadata idxMeta = idxMetaList.get(i);
				if (idxMeta.getIndex().isUnique()) {
					IndexKeyType ikt = buildIndexKeyForSelect(idxMeta, trt);
					Shard shard = new Shard(idxMeta.getSchemaId(), idxMeta.getName(), tableShard.getReplicaSetName());
					BTree tree = idxMeta.getIndexTree(shard);
					IndexCompareIndexQuery iciq = new IndexCompareIndexQuery(ikt);
					ResultIterator iter = null;
					try {
						iter = tree.find(iciq, false, pinnedBlocks);
						if (iter.hasNext()) {
							IndexKeyType ikt1 = (IndexKeyType) iter.nextEntry();
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
	
	public int updateTableRecord(String tableName, Map<String, DBType> map, Shard shard, RecordId recordId, List<BasicExpression> expList,
			TransactionId txnId) throws InvalidCollectionNameException {
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		try {
			Map<ColumnType, DBType> ctMap = convertTypes(tableName, map);
			return updateTableRecord(tableName, ctMap, shard, recordId, expList, pinnedBlocks, txnId);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public int updateTableRecord(String tableName, String objectId, Map<ColumnType, DBType> map, Shard shard) {
		RecordId recId = getRecordIdFromObjectId(tableName, objectId, shard);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		try {
			return TableRecordManager.getInstance().updateTableRecord(tableName, map, shard, recId, null, pinnedBlocks, null);
		} catch (InvalidCollectionNameException e) {
			throw new RuntimeException(e);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	private RecordId getRecordIdFromObjectId(String tableName, String objectId, Shard shard) {
		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex("objectId"+tableName);
		Shard idxShard = new Shard(idxMeta.getSchemaId(), idxMeta.getName(), shard.getReplicaSetName());
		BTree tree = idxMeta.getIndexTree(idxShard);
		List<DBType> l = new ArrayList<DBType>();
		l.add(new StringType(objectId));
		IndexKeyType ikt = new IndexKeyType(l, null);
		IndexCompareIndexQuery entry = new IndexCompareIndexQuery(ikt);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		ResultIterator iter = null;
		RecordId recId = null;
		try {
			iter = tree.find(entry, false, pinnedBlocks);
			if (iter.hasNext()) {
				ResultContent rc = iter.next();
				recId = rc.getRecordId();
			}
		} finally {
			iter.unlock();
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return recId;
	}
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
	public int updateTableRecord(String tableName, Map<ColumnType, DBType> ctMap, Shard shard, RecordId recordId, 
			List<BasicExpression> expList, Set<BlockPtr> pinnedBlocks, TransactionId txnId) throws InvalidCollectionNameException {
		// first lets delete all indexes with old value.
		Set<IndexKeyType> uIndexLock = null;
		
		CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
		CollectionAlias ca = new CollectionAlias(tableName, "");
		DataContext context = new DataContext();
		int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(tableName).getSchemaId();
		ObjectLocker.getInstance().acquireLock(recordId);
		Map<ColumnType, DBType> oldColumnValues = new HashMap<ColumnType, DBType>(ctMap.size());
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
//		if (colMeta.isLoggingEnabled()) {
			txnId = LogManager.getInstance().startTxn();
//		}

		QueriableBlockRecord tr = null;
		try {
//			List<ColumnType> selectColumns = buildSelectCoulmnList(tableName, ctMap, expList);
			
			RecordBlock rb = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedBlocks);
			if (rb == null) {
				return 0;
			}
			rb.readLock();
			try {
				tr = CacheObjectMgr.getInstance().getRecord(rb, recordId, null, schemaId, pinnedBlocks);
				if (tr == null) {
					return 0;
				}
				context.add(ca, new TableResultContent(rb, recordId, schemaId));
				if (!AndQueryExecutor.filter(context, expList)) {
					return 0;
				}				
				oldColumnValues = new HashMap<ColumnType, DBType>(tr.getColumns());
			} finally {
				if (rb != null) {
					rb.readUnlock();
				}
			}
			
			if (colMeta.getSchemaId() >= 3) {
				updateChangedValues(ctMap, oldColumnValues);
			}
			Map<ColumnType, DBType> workingCtMap = new HashMap<ColumnType, DBType>(oldColumnValues);
			workingCtMap.putAll(ctMap);
			List<IndexMetadata> changedIndexes = getChangedIndexes(tableName, ctMap);
			uIndexLock = checkForUniqueViolations(changedIndexes, tableName, workingCtMap, shard, recordId, pinnedBlocks);
			if (uIndexLock == null) {
				return 0;
			}
			
//			RecordBlock block = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedBlocks);
//			block.readLock();
//			try {
//				CacheObjectMgr.getInstance().getRecord(rb, recordId, selectColumns, schemaId, pinnedBlocks);
//				context.add(ca, new TableResultContent(block, recordId, schemaId));
//				Iterator<ColumnType> iter = ctMap.keySet().iterator();
//				while (iter.hasNext()) {
//					ColumnType ct = iter.next();
//					oldColumnValues.put(ct, context.getValue(ca, ct, null));
//				}
//				if (!AndQueryExecutor.filter(context, expList)) {
//					return 0;
//				}				
//			} finally {
//				block.readUnlock();
//			}
			TableRecord oldRecord = new TableRecord(oldColumnValues);
			
			for (int i = 0; i < changedIndexes.size(); i++) {
				IndexMetadata idxMeta = changedIndexes.get(i);
				IndexKeyType ikt = buildIndexKeyForUpdate(idxMeta, oldRecord, recordId, pinnedBlocks);
				idxMeta.getIndexTree(shard).remove(ikt, pinnedBlocks, txnId);
			}
	
			QueriableRecordList recList = SchemaMetadata.getInstance().getCollectionMetadata(tableName).getRecordList(shard);
			TableRecord record = new TableRecord(ctMap);
			recList.update(recordId, record, pinnedBlocks, txnId);
			TableRecord newRecord = new TableRecord(workingCtMap);
			for (int i = 0; i < changedIndexes.size(); i++) {
				IndexMetadata idxMeta = changedIndexes.get(i);
				IndexKeyType ikt = buildIndexKeyForUpdate(idxMeta, newRecord, recordId, pinnedBlocks);
				try {
					idxMeta.getIndexTree(shard).insert(ikt, pinnedBlocks, txnId);
				} catch (UniqueKeyViolationException e) {
					throw new RuntimeException("Unique Key violation: should never happen");
				}
			}
			
			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) && WonderDBPropertyManager.getInstance().isReplicationEnabled()) {
				Producer<String, KafkaPayload> producer = KafkaProducerManager.getInstance().getProducer(shard.getReplicaSetName());
				if (producer != null) {
					DBType dt = tr.getColumnValue(colMeta, colMeta.getColumnType("objectId"), null);
					String objectId = ((StringType) dt).get();
					KafkaPayload payload = new KafkaPayload("update", tableName, objectId, ctMap);
					KeyedMessage<String, KafkaPayload> message = new KeyedMessage<String, KafkaPayload>(shard.getReplicaSetName(), payload);
					producer.send(message);
				}
			}
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			InProcessIndexQueryIndexMgr.getInstance().done(uIndexLock);
		}
		return 1;
	}

	private void updateChangedValues(Map<ColumnType, DBType> ctMap, Map<ColumnType, DBType> oldColumnValues) {
		Iterator<ColumnType> iter = ctMap.keySet().iterator();
		while (iter.hasNext()) {
			ColumnType ct = iter.next();
			DBType dt = ctMap.get(ct);
			if (dt instanceof CollectionColumn) {
				CollectionColumn cc = (CollectionColumn) dt;
				ctMap.put(ct, oldColumnValues.get(cc.getColumnType()));
			}
		}
	}
	
	public int delete(String tableName, Shard shard, RecordId recordId, List<BasicExpression> expList) {
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		TransactionId txnId = null;
		try {
			ObjectLocker.getInstance().acquireLock(recordId);
			DataContext context = new DataContext();
			int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(tableName).getSchemaId();
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
//			if (colMeta.isLoggingEnabled()) {
				txnId = LogManager.getInstance().startTxn();
//			}

			RecordBlock block = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recordId.getPtr(), schemaId, pinnedBlocks);
			if (block == null) {
				return 0;
			}
			block.readLock();
			TableRecord tr = (TableRecord) CacheObjectMgr.getInstance().getRecord(block, recordId, null, schemaId, pinnedBlocks);
			TableRecord tmpRecord = new TableRecord(recordId, new HashMap<ColumnType, DBType>(tr.getColumns()));
			StaticTableResultContent content = new StaticTableResultContent(tmpRecord, recordId, schemaId);
			
			context.add(new CollectionAlias(tableName, ""), content);
			try {
				if (!AndQueryExecutor.filter(context, expList)) {
					return 0;
				}				
			} finally {
				block.readUnlock();
			}
			
			List<IndexMetadata> idxMetaList = SchemaMetadata.getInstance().getIndexes(tableName);
			CollectionAlias ca = new CollectionAlias(tableName, "");
			// now just walk through the results and remove it from all indexes.
			deleteRecordFromIndexes(context, idxMetaList, shard, ca, pinnedBlocks, txnId);			
			QueriableRecordList recList = SchemaMetadata.getInstance().getCollectionMetadata(tableName).getRecordList(shard);
			recList.delete(recordId, pinnedBlocks, txnId);
			
			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) && WonderDBPropertyManager.getInstance().isReplicationEnabled()) {
				Producer<String, KafkaPayload> producer = KafkaProducerManager.getInstance().getProducer(shard.getReplicaSetName());
				if (producer != null) {
					String objectId = ((StringType) tr.getColumnValue(colMeta, colMeta.getColumnType("objectId"), null)).get();
					KafkaPayload payload = new KafkaPayload("delete", tableName, objectId, null);
					KeyedMessage<String, KafkaPayload> message = new KeyedMessage<String, KafkaPayload>(shard.getReplicaSetName(), payload);
					producer.send(message);
				}
			}

		} finally {
			LogManager.getInstance().commitTxn(txnId);
			if (recordId != null) {
				ObjectLocker.getInstance().releaseLock(recordId);
			}
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		
		return 1;
	}
	
	public int delete (String collectionName, String objectId, Shard shard) {
		RecordId recId = getRecordIdFromObjectId(collectionName, objectId, shard);
		return delete(collectionName, shard, recId, null);
	}
	
	private void deleteRecordFromIndexes(DataContext context, List<IndexMetadata> idxMetaList, Shard tableShard, CollectionAlias ca, 
			Set<BlockPtr> pinnedBlocks, TransactionId txnId) {
		for (int j = 0; j < idxMetaList.size(); j++) {
			IndexMetadata idxMeta = idxMetaList.get(j);
			IndexKeyType ikt = buildIndexKeyForDelete(idxMeta, context, ca, pinnedBlocks);
			Shard shard = new Shard(idxMeta.getSchemaId(), idxMeta.getName(), tableShard.getReplicaSetName());
			idxMeta.getIndexTree(shard).remove(ikt, pinnedBlocks, txnId);
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
	private List<IndexMetadata> getChangedIndexes(String tableName, Map<ColumnType, DBType> changedColumns) {
		List<IndexMetadata> tableIndexes = SchemaMetadata.getInstance().getIndexes(tableName);
		List<IndexMetadata> retList = new ArrayList<IndexMetadata>();
		
		for (int i = 0; i < tableIndexes.size(); i++) {
			IndexMetadata idxMeta = tableIndexes.get(i);
			Set<ColumnType> idxSet = idxMeta.getIndex().getColumnNameList();
			Iterator<ColumnType> iter = idxSet.iterator();
			boolean found = false;
			while (iter.hasNext()) {
				ColumnType c = iter.next();
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

//	private RecordAndRecrdBlock getRecord(IndexMetadata idxMeta, Map<ColumnType, DBType> changedColumns, boolean writeLock, Set<BlockPtr> pinnedBlocks) {
//		
//		IndexQuery query = new IndexCompareIndexQuery(buildIndexKey(idxMeta, changedColumns, null, null));
//		ResultIterator iter = idxMeta.getIndexTree().find(query, writeLock);
//		IndexRangeScanIterator it = new IndexRangeScanIterator(iter, pinnedBlocks);
//		DataRecord record = null;
//		if (iter.hasNext()) {
//			record = (DataRecord) it.nextEntry();
//			RecordBlock rb = (RecordBlock) iter.getCurrentBlock();
//			IndexKeyType ikt = buildIndexKey(idxMeta, record.getQueriableColumns(), record, rb);
//			if (query.evaluate(ikt) == 0) {
//				RecordAndRecrdBlock rarb = new RecordAndRecrdBlock();
//				rarb.record = record;
//				rarb.recordBock = rb;
//				return rarb;
//			}
//		}
//		return null;
//	}
//	
	private IndexKeyType buildIndexKey(IndexMetadata idxMeta, QueriableBlockRecord record, RecordBlock block) {
		List<CollectionColumn> idxColList = idxMeta.getIndex().getColumnList();
		List<DBType> idxCols = new ArrayList<DBType>();
		for (int i = 0; i < idxColList.size(); i++) {
			ColumnType colType = idxColList.get(i).getColumnType();
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(idxMeta.getIndex().getCollectionName());
			idxCols.add(record.getColumnValue(colMeta, colType, null));
		}
		
		return new IndexKeyType(idxCols, record.getRecordId());
	}
	
	private IndexKeyType buildIndexKeyForUpdate(IndexMetadata idxMeta, TableRecord record, RecordId recordId, Set<BlockPtr> pinnedBlocks) {
		List<CollectionColumn> idxColList = idxMeta.getIndex().getColumnList();
		List<DBType> idxCols = new ArrayList<DBType>();
		
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(idxMeta.getIndex().getCollectionName());
		for (int i = 0; i < idxColList.size(); i++) {
			ColumnType colType = idxColList.get(i).getColumnType();
			idxCols.add(record.getColumnValue(colMeta, colType, null));
		}
		return new IndexKeyType(idxCols, recordId);
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
	
	private IndexKeyType buildIndexKeyForDelete(IndexMetadata idxMeta, DataContext context, CollectionAlias ca, Set<BlockPtr> pinnedBlocks) {
		List<CollectionColumn> idxColList = idxMeta.getIndex().getColumnList();
		List<DBType> idxCols = new ArrayList<DBType>();
		
		for (int i = 0; i < idxColList.size(); i++) {
			ColumnType colType = idxColList.get(i).getColumnType();
			idxCols.add(context.getValue(ca, colType, null));
		}
		
		return new IndexKeyType(idxCols, context.get(ca));
	}
	
	private IndexKeyType buildIndexKeyForSelect(IndexMetadata idxMeta, QueriableBlockRecord record) {
		List<CollectionColumn> idxColList = idxMeta.getIndex().getColumnList();
		List<DBType> idxCols = new ArrayList<DBType>();
		
		for (int i = 0; i < idxColList.size(); i++) {
			ColumnType colType = idxColList.get(i).getColumnType();
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(idxMeta.getIndex().getCollectionName());
			idxCols.add(record.getColumnValue(colMeta, colType, null));
		}
		
		return new IndexKeyType(idxCols, null);
	}
}
