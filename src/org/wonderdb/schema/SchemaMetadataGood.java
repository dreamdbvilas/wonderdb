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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtrList;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.factory.BlockFactory;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.BTree;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.collection.exceptions.InvalidIndexException;
import org.wonderdb.collection.impl.BTreeImpl;
import org.wonderdb.collection.impl.QueriableRecordListImpl;
import org.wonderdb.file.FileAlreadyExisits;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.query.parse.DBSelectQuery;
import org.wonderdb.query.parse.QueryParser;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.server.DreamDBPropertyManager;
import org.wonderdb.server.DreamDBServer;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;
import org.wonderdb.types.metadata.impl.SerializedCollectionMetadata;
import org.wonderdb.types.metadata.impl.SerializedCollectionMetadata.FixedColumns;
import org.wonderdb.types.metadata.impl.SerializedIndexMetadata;
import org.wonderdb.types.metadata.impl.SerializedStorageCollectionMetadata;


public class SchemaMetadataGood {
	int nextId = 0;
	private ConcurrentHashMap<String, List<IndexMetadata>> collectionIndexes = new ConcurrentHashMap<String, List<IndexMetadata>>();
	private ConcurrentHashMap<String, IndexMetadata> indexByName = new ConcurrentHashMap<String, IndexMetadata>();
	private ConcurrentHashMap<String, CollectionMetadata> collections = new ConcurrentHashMap<String, CollectionMetadata>();
	private ConcurrentHashMap<Integer, String> idToNameMap = new ConcurrentHashMap<Integer, String>();
	private static SchemaMetadataGood s_instance = new SchemaMetadataGood();
	
	private SchemaMetadataGood() {
	}
	
	public static SchemaMetadataGood getInstance() {
		return s_instance;
	}
	
	
	public void initialize() {
		String systemFileName = DreamDBPropertyManager.getInstance().getSystemFile();
		if (systemFileName == null || systemFileName.length() == 0) {
			systemFileName = "system";
		}
		
		createFileStorageMetaCollection(systemFileName);
		createCollectionMetaCollection(systemFileName);
		createIndexMetaCollection(systemFileName);
		
		long size = FilePointerFactory.getInstance().getSize(systemFileName);
		if (size > 0) {	
			loadSchemaMetadata();
		} else {
			bootstrap(systemFileName);
		}
		ClusterManagerFactory.getInstance().getClusterManager().init();
	}
	
	private CollectionMetadata createCollection(String collectionName, List<CollectionColumn> columns) throws InvalidCollectionNameException {
		CollectionMetadata colMetadata = addMetaCollection("fileStorageMetaCollection");
		colMetadata.addColumns(columns);
		return colMetadata;
	}
	
	private void createRecordList(Shard shard, CollectionMetadata meta, BlockPtr head) {
		meta.createRecordList(shard, head);		
	}
	
	private void createStorage(Shard shard, String fileName) {
		FileBlockEntryType entry = FileBlockManager.getInstance().getMetaEntry(fileName);
		addStorage(shard, entry.getBlockSize(), false);
	}
	
	private void serializeMetadata(CollectionMetadata meta, Shard shard) {
		
	}
	
	private void serializedHeadTail(Shard shard) {
		
	}
	
	
	private void createFileStorageMetaCollection(String systemFileName) {
		FileBlockEntryType entry = new FileBlockEntryType(systemFileName);
		entry = FileBlockManager.getInstance().addFileBlockEntry(entry);

		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
		list.add(new CollectionColumn("fixedColumns", 0, "bt", true, false));
		list.add(new CollectionColumn("currentFilePosn", 1, "ls", true, false));
		BlockPtr ptr = new SingleBlockPtr((byte) 0, 0);
		try {
			CollectionMetadata colMetadata = createCollection("fileStorageMetaCollection", list);
			Shard shard = new Shard(0, "", "");
			createRecordList(shard, colMetadata, ptr);
		} catch (InvalidCollectionNameException e) {
		}
	}
	
	private void createCollectionMetaCollection(String systemFileName) {		
		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
		list.add(new CollectionColumn("fixedColumns", 0, "bt", true, false));
		list.add(new CollectionColumn("headPtr", 1, "bs", true, false));
		list.add(new CollectionColumn("tailPtr", 2, "bl", true, false));
		BlockPtr ptr = new SingleBlockPtr((byte) 0, DreamDBServer.DEFAULT_BLOCK_SIZE);
		try {

			CollectionMetadata meta = createCollection("metaCollection", list);
			Shard shard = new Shard(1, "", "");
			createRecordList(shard, meta, ptr);
		} catch (InvalidCollectionNameException e) {
		}
	}
	
	private void createIndexMetaCollection(String systemFileName) {		
		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
		list.add(new CollectionColumn("fixedColumns", 0, "bt", true, false));
		list.add(new CollectionColumn("headPtr", 1, "bs", true, false));
		list.add(new CollectionColumn("rootPtr", 2, "bs", true, false));
		BlockPtr ptr = new SingleBlockPtr((byte) 0, 2*DreamDBServer.DEFAULT_BLOCK_SIZE);
		
		try {
			
			CollectionMetadata colMeta = createCollection("indexMetaCollection", list); 
			Shard shard = new Shard(2, "", "");
			createRecordList(shard, colMeta, ptr);
		} catch (InvalidCollectionNameException e) {
		}
	}
	
	private void bootstrap(String systemFile) {
		BlockFactory.getInstance().bootstrapSchemaBlocks();
		bootstrapFileStorageMetadata(systemFile);
//		bootstrapCollectionMetadata();
		bootstrapIndexMetadata();
		ClusterManagerFactory.getInstance().getClusterManager();
	}
	
	private void bootstrapFileStorageMetadata(String systemFile) {
		FileBlockEntryType entry = new FileBlockEntryType(systemFile);
		entry.addBy(100*DreamDBServer.DEFAULT_BLOCK_SIZE);
		entry.setRecordId(new RecordId(new SingleBlockPtr((byte) 0 , 0), 0));
		addStorage(entry);
//		ClusterManagerFactory.getInstance().getClusterManager().createStorage(systemFile, entry.getBlockSize(), true);

		
//		entry = FileBlockManager.getInstance().getEntry("data");
//		entry.addBy(0);
////		entry.setRecordId(new RecordId(new SingleBlockPtr((byte) 0 , 0), 0));
//		addStorage(entry);
		
	}
	
//	private void bootstrapCollectionMetadata() {
//		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata("metaCollection");
//		BlockPtr metaPtr = new SingleBlockPtr((byte) 0, DreamDBServer.DEFAULT_BLOCK_SIZE);
//		SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
//		scm.updateHead(metaPtr);
//		scm.updateTail(metaPtr);
//		scm.setFixedColumns(colMeta.getName(), (byte) 0, colMeta.getFileName(), 1, false);
//		TableRecord record = new TableRecord(scm.getColumns());
//		colMeta.getRecordList().add(record, null);
//	}

	private void bootstrapIndexMetadata() {
//		BlockPtr metaPtr = new SingleBlockPtr((byte) 0, (2*FileBlockEntryType.DEFAULT_BLOCK_SIZE));
//		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata("indexMetaCollection");
//		SerializedIndexMetadata sim = new SerializedIndexMetadata();
//		sim.updateHead(metaPtr);
//		sim.updateRoot(metaPtr);
//		sim.setIndexMetaFixedColumns(indexMeta)
//		colMeta.getRecordList().add(scm);
	}
	
	private void loadSchemaMetadata() {
		loadFileStorageMetadata();
		loadCollectionMetadata();
		loadIndexMetadata();
//		loadShards();
//		loadIndexShards();
		nextId = idToNameMap.size();
	}
	
	private void loadFileStorageMetadata() {
		String s = "select * from fileStorageMetaCollection";
		DBSelectQuery query = (DBSelectQuery) QueryParser.getInstance().parse(s);
		Shard shard = new Shard(0, "", "");
		List<Map<CollectionAlias, TableRecord>> result = query.executeAndGetTableRecord(shard);
		for (int i = 0; i < result.size(); i++) {
			Iterator<TableRecord> iter = result.get(i).values().iterator();
			while (iter.hasNext()) {
				TableRecord tr = iter.next();
				Map<ColumnType, DBType> cols = tr.getColumns();
				SerializedStorageCollectionMetadata scm = new SerializedStorageCollectionMetadata();
				scm.setColumns(cols);
				FileBlockEntryType entry = scm.getFileBlockEntryType();
				
				entry = FileBlockManager.getInstance().addFileBlockEntry(entry);
				entry.setRecordId(tr.getRecordId());				
			}
		}
	}
	
	private void loadCollectionMetadata() {
		String s = "select * from metaCollection";
		Shard shard = new Shard(1, "", "");
		DBSelectQuery query = (DBSelectQuery) QueryParser.getInstance().parse(s);
		List<Map<CollectionAlias, TableRecord>> result = query.executeAndGetTableRecord(shard);
		List<CollectionColumn> list = null;
		for (int i = 0; i < result.size(); i++) {
			Iterator<TableRecord> iter = result.get(i).values().iterator();
			while (iter.hasNext()) {
				TableRecord tr = iter.next();
			
				Map<ColumnType, DBType> cols = tr.getColumns();
				SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
				scm.setColumns(cols);
				
				list = scm.getCollectionColumns();
				FixedColumns fc = scm.getFixedColumns();
				
				CollectionMetadata colMeta = null; 
					if (fc.schemaId >= 3) {
						Shard shrd = new Shard(fc.schemaId, fc.collectionName, fc.collectionName);
						colMeta = loadCollection(shrd, fc, list);
						BlockPtr ptr = scm.getHead(shard);
						SingleBlockPtrList tail = scm.getTail(shard);
						colMeta.createRecordList(shard, ptr);
						CollectionTailMgr.getInstance().initializeCollectionData(colMeta.getSchemaId(), shard, tail);
//						createShard(shrd, null, null, null);
					} else {
						colMeta = SchemaMetadata.getInstance().getCollectionMetadata(fc.schemaId);
					}
				colMeta.setRecordId(tr.getRecordId());
			}
		}
	}
	
//	private void loadShards() {
//		List<CollectionMetadata> list = new ArrayList<CollectionMetadata>(SchemaMetadata.getInstance().getCollections().values());
//		for (int i = 0; i < list.size(); i++) {
//			CollectionMetadata colMeta = list.get(i);
//			if (colMeta.getSchemaId() < 3) {
//				continue;
//			}
////			SchemaMetadata.getInstance().createShard(shard, indexName, minVal, maxVal)
//			List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(colMeta.getName());
//			for (int j = 0; j < shards.size(); j++) {
//				Shard shard = shards.get(j);
//				if (shard.getSchemaId() < 0) {
//					shard = new Shard(colMeta.getSchemaId(), shard.getSchemaObjectName(), shard.getReplicaSetName());
//				}
//				SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
//				BlockPtr head = scm.getHead(shard);
//				colMeta.createRecordList(shard, head, null);
//				SingleBlockPtrList tail = scm.getTail(shard);
//				if (ClusterManagerFactory.getInstance().getClusterManager().isParticipating(shard)) {
//					CollectionTailMgr.getInstance().initializeCollectionData(colMeta.getSchemaId(), shard, tail);
//				}
//			}
//		}
//	}
//	
	private void loadIndexMetadata() {
		String s = "select * from indexMetaCollection";
		DBSelectQuery query = (DBSelectQuery) QueryParser.getInstance().parse(s);
		Shard shard = new Shard(2, "", "");
		List<Map<CollectionAlias, TableRecord>> result = query.executeAndGetTableRecord(shard);
		for (int i = 0; i < result.size(); i++) {
			Iterator<TableRecord> iter = result.get(i).values().iterator();
			while (iter.hasNext()) {
				TableRecord tr = iter.next();
			
				Map<ColumnType, DBType> cols = tr.getColumns();
				SerializedIndexMetadata sim = new SerializedIndexMetadata();
				sim.setColumns(cols);
				IndexMetadata idxMeta = sim.getIndexMetaFixedColumns();
//				List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(idxMeta.getIndex().getCollectionName());
//				for (int x = 0; x < shards.size(); x++) {
					try {
						add(idxMeta);
					} catch (InvalidIndexException e) {
						e.printStackTrace();
					} catch (InvalidCollectionNameException e) {
						e.printStackTrace();
					}
//				}
				idxMeta.setRecordId(tr.getRecordId());
			}
		}
	}
	
	private void loadIndexShards() {
		String s = "select * from indexMetaCollection";
		DBSelectQuery query = (DBSelectQuery) QueryParser.getInstance().parse(s);
		Shard shard = new Shard(2, "", "");
		List<Map<CollectionAlias, TableRecord>> result = query.executeAndGetTableRecord(shard);
		for (int i = 0; i < result.size(); i++) {
			Iterator<TableRecord> iter = result.get(i).values().iterator();
			while (iter.hasNext()) {
				TableRecord tr = iter.next();
			
				Map<ColumnType, DBType> cols = tr.getColumns();
				SerializedIndexMetadata sim = new SerializedIndexMetadata();
				sim.setColumns(cols);
				IndexMetadata idxMeta = sim.getIndexMetaFixedColumns();
				idxMeta = SchemaMetadata.getInstance().getIndex(idxMeta.getSchemaId());
				List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(idxMeta.getIndex().getCollectionName());
				for (int x = 0; x < shards.size(); x++) {
					Shard idxShard = new Shard(idxMeta.getSchemaId(), idxMeta.getIndex().getIndexName(), shards.get(x).getReplicaSetName());
					BlockPtr head = sim.getHead(idxShard);
					BlockPtr root = sim.getRoot(idxShard);
					try {
						addShard(idxMeta, idxShard, null, head, root, false);
					} catch (InvalidIndexException e) {
						e.printStackTrace();
					} catch (InvalidCollectionNameException e) {
						e.printStackTrace();
					}
				}
				idxMeta.setRecordId(tr.getRecordId());
			}
		}
	}
	
	public List<IndexMetadata> getIndexes(String name) {
		List<IndexMetadata> list = collectionIndexes.get(name);
		if (list == null) {
			list = new ArrayList<IndexMetadata>();
		}
		return list;
	}
	
	public IndexMetadata getIndex(String name) {
		return indexByName.get(name);
	}
	
	public IndexMetadata getIndex(int id) {
		String name = idToNameMap.get(id);
		return getIndex(name);
	}
	
	public CollectionMetadata getCollectionMetadata(int id) {
		String name = idToNameMap.get(id);
		return getCollectionMetadata(name);
	}
	
	public Map<String, CollectionMetadata> getCollections() {
		return new HashMap<String, CollectionMetadata>(collections);
	}
	
	public void add(Index index, Shard shard) throws InvalidIndexException, InvalidCollectionNameException {
		add(index, shard, FileBlockManager.getInstance().getDefaultFileName());
	}
	
	public SchemaObjectImpl getSchemaObject(int schemaId) {
		CollectionMetadata colMetadata = getCollectionMetadata(schemaId);
		if (colMetadata != null) {
			return colMetadata;
		}
		return getIndex(schemaId);
	}
	
	public void add(Index index, Shard shard, String fileName) throws InvalidIndexException, InvalidCollectionNameException {
		add(index, shard, null, null, fileName);
	}
	
	private void add(Index index, Shard shard, BlockPtr head, BlockPtr root, String fileName)  throws InvalidIndexException, InvalidCollectionNameException {
		IndexMetadata meta = new IndexMetadata(index, fileName, -1);
		add(meta, shard, fileName, head, root);
	}
	
	private void add(IndexMetadata indexMeta, Shard shard, String baseFileName, BlockPtr head, BlockPtr root) throws InvalidIndexException, InvalidCollectionNameException {
		add(indexMeta, shard, baseFileName, head, root, true);
	}
	
	private void add(IndexMetadata indexMeta) throws InvalidIndexException, InvalidCollectionNameException {
		Index index = indexMeta.getIndex();
		String collectionName = index.getCollectionName();

//		if (indexByName.get(index.getIndexName()) != null) {
//			throw new InvalidIndexException("Index already present: " + index.getIndexName());
//		}
		
		int id = indexMeta.getSchemaId();
		CollectionMetadata colMeta = getCollectionMetadata(index.getCollectionName());
		if (colMeta == null) {
			addCollection(index.getCollectionName(), null);
			colMeta = getCollectionMetadata(index.getCollectionName());
		}
		
		if (id < 0) {
			id = nextId++;
		}
		
		indexMeta.schemaId = id;
		indexByName.put(index.getIndexName(), indexMeta);
		
		List<IndexMetadata> list = collectionIndexes.get(collectionName);
		if (list == null) {
			list = new ArrayList<IndexMetadata>();
			collectionIndexes.put(collectionName, list);
		}
		list.add(indexMeta);
		idToNameMap.put(id, index.getIndexName());
//		FileBlockEntryType entry = FileBlockManager.getInstance().getEntry(shard);
	}	

	private void addShard(IndexMetadata indexMeta, Shard shard, String baseFileName, BlockPtr head, BlockPtr root, boolean serialize) throws InvalidIndexException, InvalidCollectionNameException {
		Index index = indexMeta.getIndex();
		
		int id = indexMeta.getSchemaId();
		Shard indexShard = new Shard(id, index.getIndexName(), shard.getReplicaSetName());
		if (baseFileName != null) {
			FileBlockEntryType entry = FileBlockManager.getInstance().getMetaEntry(baseFileName);
			addStorage(indexShard, entry.getBlockSize(), false);
		}

		BTree tree = new BTreeImpl(id, indexShard, index.isUnique(), head, root);
		indexMeta.addShard(indexShard, tree);
		if (serialize) {
//			String newFileName = indexMeta.getFileName() + "_index_" + id + "_" + shardId;
//			indexMeta.fileName = newFileName;
//			addStorage(-1, newFileName, entry.getBlockSize(), false);
//			indexMeta.fileId = FileBlockManager.getInstance().getId(newFileName);  
			addIndexMeta(indexMeta, indexShard, head, root);
		}
	}	
	
	private void add(IndexMetadata indexMeta, Shard shard, String baseFileName, BlockPtr head, BlockPtr root, boolean serialize) throws InvalidIndexException, InvalidCollectionNameException {
		Index index = indexMeta.getIndex();
		String collectionName = index.getCollectionName();
		
		if (indexByName.get(index.getIndexName()) != null) {
			throw new InvalidIndexException("Index already present: " + index.getIndexName());
		}
		
		int id = indexMeta.getSchemaId();
		CollectionMetadata colMeta = getCollectionMetadata(index.getCollectionName());
		if (colMeta == null) {
			addCollection(index.getCollectionName(), null);
			colMeta = getCollectionMetadata(index.getCollectionName());
		}
		
		if (id < 0) {
			id = nextId++;
		}
		
		indexMeta.schemaId = id;
		indexByName.put(index.getIndexName(), indexMeta);
		
		List<IndexMetadata> list = collectionIndexes.get(collectionName);
		if (list == null) {
			list = new ArrayList<IndexMetadata>();
			collectionIndexes.put(collectionName, list);
		}
		list.add(indexMeta);
		idToNameMap.put(id, index.getIndexName());
//		FileBlockEntryType entry = FileBlockManager.getInstance().getEntry(shard);
		Shard indexShard = new Shard(id, index.getIndexName(), shard.getReplicaSetName());
		if (baseFileName != null) {
			FileBlockEntryType entry = FileBlockManager.getInstance().getMetaEntry(baseFileName);
			addStorage(indexShard, entry.getBlockSize(), false);
		}

		BTree tree = new BTreeImpl(id, indexShard, index.isUnique(), head, root);
		indexMeta.addShard(indexShard, tree);
		if (serialize) {
//			String newFileName = indexMeta.getFileName() + "_index_" + id + "_" + shardId;
//			indexMeta.fileName = newFileName;
//			addStorage(-1, newFileName, entry.getBlockSize(), false);
//			indexMeta.fileId = FileBlockManager.getInstance().getId(newFileName);  
			addIndexMeta(indexMeta, indexShard, head, root);
		}
	}	
	
	
	
	private void addIndexMeta(IndexMetadata indexMeta, Shard shard, BlockPtr head, BlockPtr root) {

		CollectionMetadata mColMeta = getCollectionMetadata("indexMetaCollection");
		TransactionId txnId = null;
		txnId = LogManager.getInstance().startTxn();

		BlockPtr p = new SingleBlockPtr(indexMeta.getFileId(shard), 0);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		SerializedIndexMetadata serializedIndexMetadata = new SerializedIndexMetadata();
		try {
			IndexBlock ibb = BlockFactory.getInstance().createIndexBranchBlock(p, pinnedBlocks);
			long posn = FileBlockManager.getInstance().getNextBlock(shard);
	
			serializedIndexMetadata.setIndexMetaFixedColumns(indexMeta);
			BlockPtr headPtr = new SingleBlockPtr((byte) -1, -1);
			BlockPtr rootPtr = new SingleBlockPtr((byte) -1, -1);
			
			if (head != null) {
				headPtr = head;
			}

			if (root != null) {
				rootPtr = head;
			}
			
			serializedIndexMetadata.updateHeadRoot(indexMeta.getFileId(shard), headPtr, rootPtr, txnId, pinnedBlocks);
			TableRecord record = new TableRecord(null, serializedIndexMetadata.getColumns());
			Shard mShard = new Shard(1, "", "");
			mColMeta.getRecordList(mShard).add(record, null, txnId);
			indexMeta.setRecordId(record.getRecordId());
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			LogManager.getInstance().commitTxn(txnId);
		}

		try {
		} finally {
		}

	}

	private void addIndexShard(Shard shard) {

		TransactionId txnId = null;
		txnId = LogManager.getInstance().startTxn();

		byte fileId = FileBlockManager.getInstance().getId(shard);
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		SerializedIndexMetadata serializedIndexMetadata = new SerializedIndexMetadata();
		try {
			IndexBlock ibb = BlockFactory.getInstance().createIndexBranchBlock(p, pinnedBlocks);
			long posn = FileBlockManager.getInstance().getNextBlock(shard);
	
//			serializedIndexMetadata.setIndexMetaFixedColumns(indexMeta);
			BlockPtr headPtr = new SingleBlockPtr((byte) -1, -1);
			BlockPtr rootPtr = new SingleBlockPtr((byte) -1, -1);
			
			serializedIndexMetadata.updateHeadRoot(fileId, headPtr, rootPtr, txnId, pinnedBlocks);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			LogManager.getInstance().commitTxn(txnId);
		}
	}

	public CollectionMetadata addCollection(String collectionName, List<CollectionColumn> list) throws InvalidCollectionNameException {
		String fileName = DreamDBPropertyManager.getInstance().getSystemFile();
		return addCollectionForDefaultShard(collectionName, fileName, list);
	}
	
	public CollectionMetadata addMetaCollection(String collectionName) throws InvalidCollectionNameException {
		CollectionMetadata meta = collections.get(collectionName);
		if (meta != null) {
			throw new InvalidCollectionNameException(collectionName);
		}
		
		int id = nextId++;
		idToNameMap.put(id, collectionName);
		CollectionMetadata colMeta = new CollectionMetadata(collectionName, id);
		
		collections.put(colMeta.getName(), colMeta);
		colMeta.createRecordList(null);
		return colMeta;
	}
	
//	public void createShard(Shard shard, String indexName, IndexKeyType minVal, IndexKeyType maxVal) {
//		
//		CollectionMetadata colMeta = getCollectionMetadata(shard.getSchemaId());
//		if (colMeta.getSchemaId() < 3) {
//			return;
//		}
//		FileBlockEntryType entry = FileBlockManager.getInstance().getEntry(shard);
//		SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
//		BlockPtr head = new SingleBlockPtr((byte) -1, -1);
//		SingleBlockPtrList sbpl = new SingleBlockPtrList();
////		TransactionId txnId = LogManager.getInstance().startTxn();
//		if (entry == null) {
//			int blockSize = colMeta.getBlockSize();
//			addStorage(shard, blockSize, false);
//			CollectionTailMgr.getInstance().initializeCollectionTailData(shard, null);
////			
////			scm.updateHead(shard, head, txnId);
////			scm.updateTail(shard, sbpl, txnId);
//		} else {
//			head = scm.getHead(shard);
//			sbpl = scm.getTail(shard);
//			CollectionTailMgr.getInstance().initializeCollectionData(colMeta.getSchemaId(), shard, sbpl);
////			CollectionTailMgr.getInstance().updateShardHeadTail(shard);
//		}
//		colMeta.createRecordList(shard, head, null);
//		List<IndexMetadata> list = getIndexes(shard.getSchemaObjectName());
//		for (int i = 0; i < list.size(); i++) {
//			IndexMetadata idxMeta = list.get(i);
//			int blockSize = idxMeta.getBlockSize();
//			Shard idxShard = new Shard(idxMeta.getSchemaId(), idxMeta.getIndex().getIndexName(), shard.getReplicaSetName());
//			entry = FileBlockManager.getInstance().getEntry(idxShard);
//			head = null;
//			BlockPtr root = null;
//			if (entry == null) {
//				addStorage(idxShard, blockSize, false);
//				addIndexShard(idxShard);
//			} else {
//				SerializedIndexMetadata sim = new SerializedIndexMetadata();
//				head = sim.getHead(idxShard);
//				root = sim.getRoot(idxShard);
//			}
//			BTree tree = new BTreeImpl(idxMeta.getSchemaId(), idxShard, idxMeta.getIndex().isUnique(), head, root);
//			idxMeta.addShard(idxShard, tree);
//		}
//	}
	
	
	public CollectionMetadata addCollectionForDefaultShard(String collectionName, String baseFileName, List<CollectionColumn> list) throws InvalidCollectionNameException {
		if (list == null) {
			list = new ArrayList<CollectionColumn>();
		}
		list.add(0, new CollectionColumn("objectId", -1, "ss", false, true));
		CollectionMetadata colMeta = createCollection(collectionName, list);
		Shard shard = new Shard(colMeta.getSchemaId(), collectionName, collectionName);
		createStorage(shard, baseFileName);
		
		CollectionTailMgr.getInstance().initializeCollectionTailData(shard);
		byte fileId = FileBlockManager.getInstance().getId(shard);
		colMeta.createRecordList(shard);
		return colMeta;
	}
	
	private CollectionMetadata loadCollection(Shard shard, FixedColumns fc, List<CollectionColumn> columns) {
		CollectionMetadata colMeta = new CollectionMetadata(fc.collectionName, fc.schemaId);
//		colMeta.fileId = fc.fileId;
		colMeta.memoryOnly = fc.memoryOnly;
//		colMeta.fileName = fc.fileName;
		colMeta.addColumns(columns);
		idToNameMap.put(colMeta.getSchemaId(), colMeta.getName());
		
		collections.put(colMeta.getName(), colMeta);
		idToNameMap.put(colMeta.getSchemaId(), colMeta.getName());
//		colMeta.createRecordList(shard, head, null);
		return colMeta;
	}
	
	public synchronized CollectionMetadata getCollectionMetadata(String colName) {
		return collections.get(colName);
	}
	
	public synchronized int addStorage(Shard shard, int blockSize, boolean isDefault) {
		String fileName = FileBlockManager.getInstance().getFileName(shard);
		FileBlockEntryType entry = FileBlockManager.getInstance().getEntry(shard);
		if (entry == null) {
			entry = new FileBlockEntryType(fileName, blockSize, 0);
			entry.setFileId((byte) -1);
			addStorage(entry);
		} else {
			throw new FileAlreadyExisits();
		}
		return entry.getFileId();
	}
	
	private void addStorage(FileBlockEntryType entry) {
		entry = FileBlockManager.getInstance().addFileBlockEntry(entry);
		CollectionMetadata colMeta = getCollectionMetadata("fileStorageMetaCollection");
		SerializedStorageCollectionMetadata serializedStorage = new SerializedStorageCollectionMetadata();
		serializedStorage.setFileBlockEntryType(entry);
		serializedStorage.updateCurrentFilePosn(entry.getCurrentPosn());
		TableRecord record = new TableRecord(null, serializedStorage.getColumns());
		TransactionId txnId = null;
		txnId = LogManager.getInstance().startTxn();
		try {
			colMeta.getRecordList(new Shard(0, "", "")).add(record, null, txnId);
		} finally {
			LogManager.getInstance().commitTxn(txnId);
		}
		entry.setRecordId(record.getRecordId());
		FreeBlocksMgr.getInstance().addStorage(entry.getFileId());
	}
}
