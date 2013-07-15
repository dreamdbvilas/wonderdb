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
package org.wonderdb.query.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.collection.exceptions.InvalidIndexException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.parser.UQLParser.CreateIndexStmt;
import org.wonderdb.parser.UQLParser.IndexCol;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;


public class CreateIndexQuery extends BaseDBQuery {
	String idxName;
	String collectionName;
//	List<CollectionColumn> idxColumns = new ArrayList<CollectionColumn>();
	CreateIndexStmt stmt;
	
	public CreateIndexQuery(String query, CreateIndexStmt stmt) {
		super(query, -1, null);
		this.stmt = stmt;
		idxName = stmt.idxName;
		collectionName = stmt.table;
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public String getIndexName() {
		return idxName;
	}
	
	public void execute() throws InvalidCollectionNameException, InvalidIndexException {
		List<CollectionColumn> idxColumns = new ArrayList<CollectionColumn>();
		List<CollectionColumn> collectionColumns = new ArrayList<CollectionColumn>();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
			throw new InvalidIndexException("collection not created..." + collectionName);
		}
		
		colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		CollectionColumn cc = null;
		for (IndexCol idxCol : stmt.colList) {
			String colName = idxCol.col;
			String type = getType(idxCol.type);
			String cName = colName;
			if (colName.contains(":")) {
				cName = extractColumnName(colName);
				colName = colName.substring(1);
				colName = colName.substring(0, colName.length()-1);
//					type = "ss";
				cc = new CollectionColumn(colMeta, cName, "ss", true, true);
				collectionColumns.add(cc);
				cc = new CollectionColumn(colMeta, colName, type, true, false);
				idxColumns.add(cc);
			} else {
				cc = new CollectionColumn(colMeta, cName, type, true, true);
				idxColumns.add(cc);
			}
		}
		List<CollectionColumn> newColumns = new ArrayList<CollectionColumn>();
		newColumns.addAll(colMeta.addColumns(collectionColumns));
		newColumns.addAll(colMeta.addColumns(idxColumns));
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		if (newColumns.size() > 0) {
			Map<ColumnType, DBType> map = new HashMap<ColumnType, DBType>();
			for (int i = 0; i < newColumns.size(); i++) {
				cc = newColumns.get(i);
//				cc = SerializedCollectionMetadata.adjustId(cc);					
				map.put(cc.getColumnType(), cc);
			}
			TransactionId txnId = LogManager.getInstance().startTxn();
			try {
				Shard shard = new Shard(1, "", "");
				TableRecordManager.getInstance().updateTableRecord("metaCollection", map, shard, colMeta.getRecordId(), null, pinnedBlocks, txnId);
				LogManager.getInstance().commitTxn(txnId);
			} catch (InvalidCollectionNameException e) {
			} finally {
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
		Index idx = new Index(idxName, collectionName, idxColumns, stmt.unique, true);
		String storageFile = null;
		if (stmt.storage != null && stmt.storage.length() > 0) {
			stmt.storage = stmt.storage.substring(1, stmt.storage.length()-1);
			storageFile = stmt.storage;
		} else {
			storageFile = WonderDBPropertyManager.getInstance().getSystemFile();			
		}

		List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(collectionName);
		for (int i = 0; i < shards.size(); i++) {
			SchemaMetadata.getInstance().add(idx, shards.get(i), storageFile);
		}
		
		ClusterManagerFactory.getInstance().getClusterManager().createIndex(collectionName, idxName, storageFile, idx.getColumnList(), idx.isUnique(), true);
	}
	
	public static String extractColumnName(String cName) {
		String[] s = cName.split("://");
		if (s.length == 1) {
			return s[0];
		} 
		
		if (s.length == 2) {
			String x = s[1];
			String x1[] = x.split("/");
			return x1[0];
		}
		return null;
	}

	public static String getType(String s) {
		if (s.equals("int")) {
			return "is";
		}
		if (s.equals("long")) {
			return "ls";
		}
		if (s.equals("float")) {
			return "fs";
		}
		if (s.equals("double")) {
			return "ds";
		}
		if (s.equals("string")) {
			return "ss";
		}
		throw new RuntimeException("Invalid type");
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
