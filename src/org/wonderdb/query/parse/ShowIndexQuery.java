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

import java.util.List;

import org.wonderdb.expression.AndExpression;
import org.wonderdb.metadata.StorageMetadata;
import org.wonderdb.parser.ShowIndexStmt;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.IndexNameMeta;


public class ShowIndexQuery extends BaseDBQuery {
	String indexName;
	
	public ShowIndexQuery(String q, ShowIndexStmt stmt){
		super(q, null, -1, null);
		indexName = stmt.indexName;
	}
	
	
	public String execute() {
		IndexNameMeta idxMeta = SchemaMetadata.getInstance().getIndex(indexName);
		if (idxMeta == null) {
			return "\n";
		}
		byte fileId = idxMeta.getHead().getFileId();
		String storage = StorageMetadata.getInstance().getFileName(fileId);
		
		StringBuilder builder = new StringBuilder();
		builder.append("Index Name: ").append(indexName).append("\n");
		builder.append("Collection Name: ").append(idxMeta.getCollectionName()).append("\n");
		builder.append("Is unique: ").append(idxMeta.isUnique()).append("\n");
		List<Integer> cols = idxMeta.getColumnIdList();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(idxMeta.getCollectionName());
		for (Integer col : cols) {
			ColumnNameMeta cnm = colMeta.getCollectionColumn(col);
			builder.append(cnm.getColumnName())
			.append("\t")
			.append(cnm.getColumnType())
			.append("\n");
		}
		builder.append("STORAGE:	").append(storage).append("\n");
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
		return s;
//		throw new RuntimeException("Invalid type");
	}


	@Override
	public AndExpression getExpression() {
		return null;
	}
}
