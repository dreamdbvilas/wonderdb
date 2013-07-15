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
import java.util.List;

import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.parser.UQLParser.CreateTableStmt;
import org.wonderdb.parser.UQLParser.IndexCol;
import org.wonderdb.schema.ClusterSchemaManager;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;


public class CreateTableQuery extends BaseDBQuery {
	String collectionName;
	List<CollectionColumn> idxColumns = new ArrayList<CollectionColumn>();
	CreateTableStmt stmt;
	
	public CreateTableQuery(String query, CreateTableStmt stmt){
		super(query, -1, null);
		this.stmt = stmt;
		collectionName = stmt.tableName;
	}
	
	public void execute() throws InvalidCollectionNameException {
		CollectionColumn cc = new CollectionColumn("objectId", -1, "ss", false, false);
		idxColumns.add(cc);
		for (IndexCol idxCol : stmt.colList) {
			String colName = idxCol.col;
			String type = idxCol.type;
			cc = new CollectionColumn(colName, -1, getType(type), true, true);
			idxColumns.add(cc);
		}
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
			String fileName = FileBlockManager.getInstance().getDefaultFileName();
			if (stmt.storage != null && stmt.storage.length() > 0) {
				stmt.storage = stmt.storage.substring(1, stmt.storage.length()-1);
				fileName = stmt.storage;
			}
			ClusterSchemaManager.getInstance().createCollection(collectionName, fileName, idxColumns, true);
		}
	}
	
	public String getCollectionName() {
		return collectionName;
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
