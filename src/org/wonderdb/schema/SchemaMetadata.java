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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.core.collection.impl.BTreeImpl;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.metadata.StorageMetadata;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.storage.FileBlockManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.CollectionNameMeta;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.IndexRecordMetadata;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.ObjectListRecord;


public class SchemaMetadata {
	int nextId = 0;
	private ConcurrentHashMap<String, List<IndexNameMeta>> collectionIndexes = new ConcurrentHashMap<String, List<IndexNameMeta>>();
	private ConcurrentHashMap<String, IndexNameMeta> indexByName = new ConcurrentHashMap<String, IndexNameMeta>();
	private ConcurrentHashMap<String, CollectionMetadata> collections = new ConcurrentHashMap<String, CollectionMetadata>();
	private ConcurrentMap<String, String> functions = new ConcurrentHashMap<String, String>();
	
	WonderDBList objectList = null; // name, head, 
	WonderDBList objectColumnList = null; // id, name, tablename, type, isnull
	WonderDBList indexList = null; // id name tableName, columnlist
	
	private static SchemaMetadata s_instance = new SchemaMetadata();
	
	private SchemaMetadata() {
	}
	
	public String getColumnName(String collectionName, int columnId) {
		return null;
	}
	
	public static SchemaMetadata getInstance() {
		return s_instance;
	}

	public void init(boolean isNew) {
		if (isNew) {
			create();
		} else {
			load();
		}
	}
	
	public List<CollectionMetadata> getCollections() {
		return new ArrayList<CollectionMetadata>(collections.values());
	}
	
	public TypeMetadata getTypeMetadata(String collectionName) {
		CollectionMetadata colMeta = collections.get(collectionName);
		if (colMeta == null) {
			IndexNameMeta inm = indexByName.get(collectionName);
			if (inm != null) {
				return getIndexMetadata(inm);
			}
		}
		List<ColumnNameMeta> list = colMeta.getCollectionColumns();
		if (list.size() == 1 && "_internal".equals(list.get(0).getColumnName())) {
			return new ColumnSerializerMetadata(list.get(0).getColumnType());
		}
		
		TableRecordMetadata meta = new TableRecordMetadata();
		Map<Integer, Integer> columnIdTypeMap = new HashMap<Integer, Integer>();
		for (int i = 0; i < list.size(); i++) {
			ColumnNameMeta cnm = list.get(i);
			columnIdTypeMap.put(cnm.getCoulmnId(), cnm.getColumnType());
		}
		meta.setColumnIdTypeMap(columnIdTypeMap);
		return meta;
	}
	
	public CollectionMetadata getCollectionMetadata(String collectionName) {
		return collections.get(collectionName);
	}
	
	public TypeMetadata getIndexMetadata(IndexNameMeta meta) {
		IndexRecordMetadata irm = new IndexRecordMetadata();
		if(meta.getCollectionName() == null || meta.getCollectionName().length() == 0) {
			irm.setColumnIdList(meta.getColumnIdList());
			irm.setTypeList(meta.getColumnIdList());
		} else {
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(meta.getCollectionName());
			List<Integer> colTypeList = new ArrayList<Integer>();
			List<Integer> colIdList = new ArrayList<Integer>();
			for (int i = 0; i < meta.getColumnIdList().size(); i++) {
				int colId = meta.getColumnIdList().get(i);
				ColumnNameMeta cnm = colMeta.columnIdToNameMap.get(colId);
				colTypeList.add(cnm.getColumnType());
				colIdList.add(colId);
			}
			irm.setTypeList(colTypeList);
			irm.setColumnIdList(colIdList);
		}
		return irm;
	}
	
	public void addColumns(String collectionName, List<ColumnNameMeta> columns) {
		CollectionMetadata colMeta = getCollectionMetadata(collectionName);
		List<ColumnNameMeta> newColumns = colMeta.addColumns(columns);
		if (newColumns.size() > 0) {
			TransactionId txnId = LogManager.getInstance().startTxn();
			Set<Object> pinnedBlocks = new HashSet<Object>();
			try {
				for (int i = 0; i < newColumns.size(); i++) {
					DBType column = newColumns.get(i);
					ListRecord record = new ObjectListRecord(column);
					objectColumnList.add(record, txnId, new ColumnSerializerMetadata(SerializerManager.COLUMN_NAME_META_TYPE), pinnedBlocks);
				}
			} finally {
				LogManager.getInstance().commitTxn(txnId);
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
	}
	
	private void create() {
		TransactionId txnId = LogManager.getInstance().startTxn();
		Set<Object> pinnedBlocks = new HashSet<Object>();
		try {
			BlockPtr ptr = new SingleBlockPtr((byte) 0, 1*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			objectList = WonderDBList.create("_object", ptr, 1, txnId, pinnedBlocks);
			
			ptr = new SingleBlockPtr((byte) 0, 2*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			objectColumnList = WonderDBList.create("_column", ptr, 1, txnId, pinnedBlocks);
			
			ptr = new SingleBlockPtr((byte) 0, 3*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			indexList = WonderDBList.create("_index", ptr, 1, txnId, pinnedBlocks);
									
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	private void load() {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		
		try {
			Map<String, CollectionNameMeta> map = new HashMap<String, CollectionNameMeta>();
			Map<String, List<ColumnNameMeta>> collectionColumnMap = new HashMap<String, List<ColumnNameMeta>>();
			
			BlockPtr ptr = new SingleBlockPtr((byte) 0, 1*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			objectList = WonderDBList.load("_object", ptr, 1, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), pinnedBlocks);
			ResultIterator iter = objectList.iterator(new ColumnSerializerMetadata(SerializerManager.COLLECTION_NAME_META_TYPE), pinnedBlocks);
			try {
				while (iter.hasNext()) {
					ObjectListRecord record = (ObjectListRecord) iter.next();
					DBType column = record.getColumn();
					CollectionNameMeta cnm = (CollectionNameMeta) column;
					cnm.setRecordId(record.getRecordId());
					map.put(cnm.getName(), cnm);
				}
			} finally {
				iter.unlock(true);
			}
			
			ptr = new SingleBlockPtr((byte) 0, 2*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			objectColumnList = WonderDBList.load("_column", ptr, 1, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), pinnedBlocks);
			iter = objectColumnList.iterator(new ColumnSerializerMetadata(SerializerManager.COLUMN_NAME_META_TYPE), pinnedBlocks);
			try {
				while (iter.hasNext()) {
					ObjectListRecord record = (ObjectListRecord) iter.next();
					DBType column = record.getColumn();
					ColumnNameMeta cnm = (ColumnNameMeta) column;
					cnm.setRecId(record.getRecordId());
					List<ColumnNameMeta> list = collectionColumnMap.get(cnm.getCollectioName());
					if (list == null) {
						list = new ArrayList<ColumnNameMeta>();
						collectionColumnMap.put(cnm.getCollectioName(), list);
					}
					list.add(cnm);
				}
			} finally {
				iter.unlock(true);
			}
			
			Iterator<CollectionNameMeta> iter1 = map.values().iterator();
			while (iter1.hasNext()) {
				CollectionNameMeta meta = iter1.next();
				List<ColumnNameMeta> columns = collectionColumnMap.get(meta.getName());				
				try {
					String storageFile = StorageMetadata.getInstance().getFileName(meta.getHead().getFileId());
					CollectionMetadata colMeta = createCollection(meta.getName(), storageFile, columns);
					WonderDBList dbList = WonderDBList.load(meta.getName(), meta.getHead(), meta.getConcurrency(), 
							getTypeMetadata(meta.getName()), pinnedBlocks);
					colMeta.setDBList(dbList);
				} catch (InvalidCollectionNameException e) {
					e.printStackTrace();
				}
			}

			ptr = new SingleBlockPtr((byte) 0, 3*WonderDBPropertyManager.getInstance().getDefaultBlockSize());
			indexList = WonderDBList.load("_index", ptr, 1, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), pinnedBlocks);
			ResultIterator iter2 = indexList.iterator(new ColumnSerializerMetadata(SerializerManager.INDEX_NAME_META_TYPE), pinnedBlocks);
			try {
				while (iter2.hasNext()) {
					ObjectListRecord record = (ObjectListRecord) iter2.next(); 
					IndexNameMeta inm = (IndexNameMeta) record.getColumn(); 
					TypeMetadata meta = getIndexMetadata(inm);
					BTree tree = BTreeImpl.load(inm.isUnique(), inm.getHead(), meta, pinnedBlocks);
					inm.setTree(tree);
					indexByName.put(inm.getIndexName(), inm);
					List<IndexNameMeta> list = collectionIndexes.get(inm.getCollectionName());
					if (list == null) {
						list = new ArrayList<IndexNameMeta>();
						collectionIndexes.put(inm.getCollectionName(), list);
					}
					list.add(inm);
				}
			} finally {
				iter2.unlock(true);
			}
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public WonderDBList createNewList(String id, int concurrentSize, ColumnSerializerMetadata meta) throws InvalidCollectionNameException {
		ColumnNameMeta cnm = new ColumnNameMeta();
		cnm.setCollectioName(id);
		cnm.setColumnName("_internal");
		cnm.setColumnType(meta.getColumnId());
		List<ColumnNameMeta> list = new ArrayList<ColumnNameMeta>();
		list.add(cnm);
		
		CollectionMetadata colMeta = createNewCollection(id, null, list, concurrentSize);
		return colMeta.getRecordList(new Shard(""));
	}
	
	public CollectionMetadata createNewCollection(String collectionName, String fileName, List<ColumnNameMeta> columns, int concurrentSize) throws InvalidCollectionNameException {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		TransactionId txnId = LogManager.getInstance().startTxn();
		try {
			String storageFile = StorageMetadata.getInstance().getDefaultFileName();
			byte fileId = StorageMetadata.getInstance().getDefaultFileId();
			if (fileName != null) {
				fileId = StorageMetadata.getInstance().getFileId(fileName);
				storageFile = fileName;
			}
			CollectionMetadata colMeta = createCollection(collectionName, storageFile, columns);
			long posn = FileBlockManager.getInstance().getNextBlock(fileId);
			BlockPtr ptr = new SingleBlockPtr(fileId, posn);
			WonderDBList dbList = WonderDBList.create(collectionName, ptr, concurrentSize, txnId, pinnedBlocks);
			colMeta.setDBList(dbList);
			
			CollectionNameMeta cnm = new CollectionNameMeta();
			cnm.setConcurrency(10);
			cnm.setHead(ptr);
			cnm.setLoggingEnabled(colMeta.isLoggingEnabled);
			cnm.setName(collectionName);
			ObjectListRecord record = new ObjectListRecord(cnm);
			objectList.add(record, txnId, new ColumnSerializerMetadata(SerializerManager.COLLECTION_NAME_META_TYPE), pinnedBlocks);
			
			if (columns != null) {
				for (int i = 0; i < columns.size(); i++) {
					ColumnNameMeta columnNameMeta = columns.get(i);
					record = new ObjectListRecord(columnNameMeta);
					objectColumnList.add(record, txnId, new ColumnSerializerMetadata(SerializerManager.COLUMN_NAME_META_TYPE), pinnedBlocks);
				}
			}			
			return colMeta;
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public BTree createBTree(String name, boolean unique, int type) {
		List<Integer> columnIdList = new ArrayList<Integer>();
		columnIdList.add(type);
		
		IndexNameMeta inm = new IndexNameMeta();
		inm.setAscending(true);
		inm.setCollectionName("");
		inm.setColumnIdList(columnIdList);
		inm.setIndexName(name);
		inm.setUnique(unique);
		
		String storageFile = FileBlockManager.getInstance().getDefaultFileName();
		IndexNameMeta m = createNewIndex(inm, storageFile);
		return m.getTree();
	}
	
	
	public IndexNameMeta createNewIndex(IndexNameMeta inm, String storageFile) {
		IndexNameMeta temp =indexByName.putIfAbsent(inm.getIndexName(), inm);
		if (temp != null) {
			return temp;
		}
		TransactionId txnId = LogManager.getInstance().startTxn();
		Set<Object> pinnedBlocks = new HashSet<Object>();
		try {
			byte fileId = StorageMetadata.getInstance().getFileId(storageFile);
			long posn = FileBlockManager.getInstance().getNextBlock(fileId);
			BlockPtr head = new SingleBlockPtr(fileId, posn);
			BTree tree = BTreeImpl.create(inm.isUnique(), head, 
					getIndexMetadata(inm), txnId, pinnedBlocks);
			inm.setTree(tree);
			inm.setHead(head);
			
			ObjectListRecord record = new ObjectListRecord(inm);
			indexList.add(record, txnId, new ColumnSerializerMetadata(SerializerManager.INDEX_NAME_META_TYPE), pinnedBlocks);
			List<IndexNameMeta> list = collectionIndexes.get(inm.getCollectionName());
			if (list == null) {
				list = new ArrayList<IndexNameMeta>();
				collectionIndexes.put(inm.getCollectionName(), list);
			}
			list.add(inm);
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return inm;
	}
	
	private CollectionMetadata createCollection(String collectionName, String storageName, List<ColumnNameMeta> columns) throws InvalidCollectionNameException {
		CollectionMetadata colMeta = new CollectionMetadata(collectionName);
		CollectionMetadata meta = collections.putIfAbsent(collectionName, colMeta);
		if (meta != null) {
			throw new InvalidCollectionNameException("collection already exists: " + collectionName);
		}
		
		colMeta.addColumns(columns);
		collections.put(collectionName, colMeta);
		return colMeta;
	}

	public List<IndexNameMeta> getIndexes(String name) {
		List<IndexNameMeta> list = collectionIndexes.get(name);
		if (list == null) {
			list = new ArrayList<IndexNameMeta>();
		}
		return list;
	}
	
	public IndexNameMeta getIndex(String name) {
		return indexByName.get(name);
	}
}
