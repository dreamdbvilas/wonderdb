package org.wonderdb.query.parse;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.exception.InvalidIndexException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.metadata.StorageMetadata;
import org.wonderdb.parser.jtree.Node;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.ObjectListRecord;


public class CreateIndexQuery extends BaseDBQuery {
	String idxName;
	String collectionName;
	List<String> colList = new ArrayList<>();
	boolean unique = false;
	String storageName = null;
	
	public CreateIndexQuery(String q, Node qry) {
		super(q, (SimpleNode) qry, -1, null);
		SimpleNode query = (SimpleNode) qry;
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTINDEXNAME);
		if (node == null) {
			throw new RuntimeException("Invalid syntax");
		}
		idxName = node.jjtGetFirstToken().image;
		
		node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTTABLENAME);
		if (node == null) {
			throw new RuntimeException("Invalid syntax");
		}
		collectionName = node.jjtGetFirstToken().image;
		
		List<SimpleNode> colNameNodes = new ArrayList<>();
		SimpleNodeHelper.getInstance().getNodes(query, UQLParserTreeConstants.JJTIDENTIFIER, colNameNodes);
		if (colNameNodes.size() <= 0) {
			throw new RuntimeException("Invalid syntax");
		}
		
		for (int i = 0; i < colNameNodes.size(); i++) {
			node = colNameNodes.get(i);
			colList.add(node.jjtGetFirstToken().image);
		}
		
		node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTUNIQUE);
		if (node != null) {
			unique = true;
		}
		
		node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTSTORAGENAME);
		if (node == null) {
			storageName = StorageMetadata.getInstance().getDefaultFileName();
		} else {
			storageName = node.jjtGetFirstToken().image;
		}
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public String getIndexName() {
		return idxName;
	}
	
	public void execute() throws InvalidCollectionNameException, InvalidIndexException {
		List<Integer> idxColumns = new ArrayList<Integer>();
		List<ColumnNameMeta> collectionColumns = new ArrayList<>();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
			throw new InvalidIndexException("collection not created..." + collectionName);
		}
		
		ColumnNameMeta cnm = null;
		for (String idxCol : colList) {
			String colName = idxCol;
			cnm = new ColumnNameMeta();
			cnm.setCollectioName(collectionName);
			cnm.setColumnName(colName);
			int type = colMeta.getColumnType(idxCol);
			cnm.setColumnType(type);
			collectionColumns.add(cnm);
			idxColumns.add(cnm.getCoulmnId());
		}

		List<ColumnNameMeta> newColumns = colMeta.addColumns(collectionColumns);
		Set<Object> pinnedBlocks = new HashSet<>();
		WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getRecordList(new Shard(""));
		if (newColumns.size() > 0) {
			TransactionId txnId = LogManager.getInstance().startTxn();
			try {
				for (int i = 0; i < newColumns.size(); i++) {
					ListRecord record = new ObjectListRecord(newColumns.get(i));
					dbList.add(record, txnId, new ColumnSerializerMetadata(SerializerManager.COLUMN_NAME_META_TYPE), pinnedBlocks);
				}
			} finally {
				LogManager.getInstance().commitTxn(txnId);
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
		IndexNameMeta inm = new IndexNameMeta();
		inm.setCollectionName(collectionName);
		inm.setIndexName(idxName);
		inm.setColumnIdList(idxColumns);
		inm.setUnique(unique);
		inm.setAscending(true);

		List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(collectionName);
		for (int i = 0; i < shards.size(); i++) {
			SchemaMetadata.getInstance().createNewIndex(inm, storageName);
		}
		
//		ClusterManagerFactory.getInstance().getClusterManager().createIndex(collectionName, idxName, storageFile, inm.g.getColumnList(), idx.isUnique(), true);
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

	public static int getType(String s) {
		if (s.equals("int")) {
			return SerializerManager.INT;
		}
		if (s.equals("long")) {
			return SerializerManager.LONG;
		}
		if (s.equals("float")) {
			return SerializerManager.FLOAT;
		}
		if (s.equals("double")) {
			return SerializerManager.DOUBLE;
		}
		if (s.equals("string")) {
			return SerializerManager.STRING;
		}
		throw new RuntimeException("Invalid type");
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
