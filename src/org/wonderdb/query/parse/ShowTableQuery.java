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
import java.util.Iterator;
import java.util.List;

import org.wonderdb.expression.AndExpression;
import org.wonderdb.parser.UQLParser.ShowIndexStmt;
import org.wonderdb.parser.UQLParser.ShowTableStmt;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.impl.ColumnType;


public class ShowTableQuery extends BaseDBQuery {
	String collectionName;
	
	public ShowTableQuery(String query, ShowTableStmt stmt){
		super(query, -1, null);
		collectionName = stmt.tableName;
	}
	
	
	public String execute() {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
			return "\n";
		}
		StringBuilder builder = new StringBuilder();
		
		List<ColumnType> cols = new ArrayList<ColumnType>(colMeta.getQueriableColumns().keySet());
		Iterator<ColumnType> iter = cols.iterator();
		while (iter.hasNext()) {
			ColumnType ct = iter.next();
			CollectionColumn cc = colMeta.getCollectionColumn((Integer) ct.getValue());
			if (cc.isQueriable()) {
				builder.append(colMeta.getColumnName((Integer) ct.getValue()) )
				.append("\t")
				.append(getType(colMeta.getColumnSerializerName(ct)))
				.append("\n");
			}
		}
		
		builder.append("\n");
		builder.append("STORAGE: ").append("\n");
		builder.append("\n");

		List<IndexMetadata> list = SchemaMetadata.getInstance().getIndexes(collectionName);
		for (IndexMetadata idxMeta : list) {
			builder.append("\n");
			ShowIndexStmt stmt = new ShowIndexStmt();
			stmt.indexName = idxMeta.getName();
			ShowIndexQuery q = new ShowIndexQuery(stmt);
			builder.append(q.execute());
		}
		return builder.toString();
	}
	
	public static String getType(String s) {
		if (s.equals("is")) {
			return "int";
		}
		if (s.equals("ls")) {
			return "long";
		}
		if (s.equals("fs")) {
			return "float";
		}
		if (s.equals("ds")) {
			return "double";
		}
		if (s.equals("ss")) {
			return "string";
		}
		if (s.equals("bs")) {
			return "byte type";
		}
		return s;
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
