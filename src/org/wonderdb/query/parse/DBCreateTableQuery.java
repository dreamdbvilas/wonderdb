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
import java.util.List;

import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.metadata.StorageMetadata;
import org.wonderdb.parser.jtree.Node;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.storage.FileBlockManager;
import org.wonderdb.types.ColumnNameMeta;



public class DBCreateTableQuery extends BaseDBQuery {
	String collectionName;
	List<ColumnNameMeta> idxColumns = new ArrayList<ColumnNameMeta>();
	String fileName = null;
	
	public DBCreateTableQuery(String q, Node qry) {
		super(q, (SimpleNode) qry, -1, null);
		SimpleNode query = (SimpleNode) qry;
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTTABLENAME);
		if (node == null) {
			throw new RuntimeException("Invalid syntax:");
		}
		collectionName = node.jjtGetFirstToken().image;
		
		List<SimpleNode> columnNameNodeList = new ArrayList<SimpleNode>();
//		List<SimpleNode> columnTypeNodeList = new ArrayList<SimpleNode>();
		
		SimpleNodeHelper.getInstance().getNodes(query, UQLParserTreeConstants.JJTTABLECOLUMNNAME, columnNameNodeList);
		
		if (columnNameNodeList == null || columnNameNodeList.size() == 0) {
			throw new RuntimeException("Invalid syntax:");
		}
		
		ColumnNameMeta cc = null;
		for (int i = 0; i < columnNameNodeList.size(); i++) {
			String columnName = columnNameNodeList.get(i).jjtGetFirstToken().image;
			int type = getType(columnNameNodeList.get(i).jjtGetFirstToken().next.image);
			cc = new ColumnNameMeta();
			cc.setCollectioName(collectionName);
			cc.setColumnName(columnName);
			cc.setColumnType(type);
			idxColumns.add(cc);
		}
		
		fileName = StorageMetadata.getInstance().getDefaultFileName();
		node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTSTORAGENAME);
		if (node != null) {
			fileName = node.jjtGetFirstToken().image.substring(1, node.jjtGetFirstToken().image.length()-1);
		}		
	}
	
	public DBCreateTableQuery(String q, String collectionName, List<ColumnNameMeta> columns, String storage) {
		super(q, null, -1, null);
		init(columns, storage);
	}

	private void init(List<ColumnNameMeta> columns, String storage) {
		idxColumns = columns;
		
		fileName = FileBlockManager.getInstance().getDefaultFileName();
		if (storage != null && storage.length() > 0) {
			fileName = storage;
		}		
	}
	
	
	
	public void execute() throws InvalidCollectionNameException {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
//			ClusterSchemaManager.getInstance().createCollection(collectionName, fileName, idxColumns, true);
			colMeta = SchemaMetadata.getInstance().createNewCollection(collectionName, fileName, idxColumns, 10);		
		}
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	@Override
	public AndExpression getExpression() {
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
}
