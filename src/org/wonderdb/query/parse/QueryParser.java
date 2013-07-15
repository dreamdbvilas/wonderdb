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

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.parser.UQLParser;
import org.wonderdb.parser.UQLParser.AddToReplicaSetStmt;
import org.wonderdb.parser.UQLParser.CreateIndexStmt;
import org.wonderdb.parser.UQLParser.CreateReplicaSetStmt;
import org.wonderdb.parser.UQLParser.CreateShardStmt;
import org.wonderdb.parser.UQLParser.CreateStorageStmt;
import org.wonderdb.parser.UQLParser.CreateTableStmt;
import org.wonderdb.parser.UQLParser.DeleteStmt;
import org.wonderdb.parser.UQLParser.InsertStmt;
import org.wonderdb.parser.UQLParser.SelectStmt;
import org.wonderdb.parser.UQLParser.ShowIndexStmt;
import org.wonderdb.parser.UQLParser.ShowSchemaStmt;
import org.wonderdb.parser.UQLParser.ShowTableStmt;
import org.wonderdb.parser.UQLParser.UpdateStmt;
import org.wonderdb.types.DBType;




public class QueryParser {
	private static QueryParser parser = new QueryParser();
	private QueryParser() {
	}
	public static QueryParser getInstance() {
		return parser;
	}

	public DBQuery parse(String query) {
		return parse(query, new ArrayList<DBType>(), -1, null);
	}
	
	public DBQuery parse(String query, List<DBType> bindParamList, int type, ChannelBuffer buffer) {
		UQLParser parser = new UQLParser(query);
		Object o = null;
		try {
			o = parser.parse();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		DBQuery dbQuery = null;
		if (o instanceof SelectStmt) {
			SelectStmt stmt = (SelectStmt) o;
			DBSelectQuery q = new DBSelectQuery(query, stmt, bindParamList, type, buffer);
			return q;
		}
		if (o instanceof InsertStmt) {
			InsertStmt stmt = (InsertStmt) o;
			dbQuery = new DBInsertQuery(query, stmt, bindParamList, type, buffer);
		}
		if (o instanceof CreateIndexStmt) {
			CreateIndexStmt stmt = (CreateIndexStmt) o;
			dbQuery = new CreateIndexQuery(query, stmt);
		}
		if (o instanceof CreateTableStmt) {
			CreateTableStmt stmt = (CreateTableStmt) o;
			dbQuery = new CreateTableQuery(query, stmt);
		}
		if (o instanceof UpdateStmt) {
			UpdateStmt stmt = (UpdateStmt) o;
			dbQuery = new DBUpdateQuery(query, stmt, bindParamList, type, buffer);
		}
		
		if (o instanceof DeleteStmt) {
			DeleteStmt stmt = (DeleteStmt) o;
			dbQuery = new DBDeleteQuery(query, stmt, bindParamList, type, buffer);
		}
		
		if (o instanceof ShowTableStmt) {
			ShowTableStmt stmt = (ShowTableStmt) o;
			dbQuery = new ShowTableQuery(query, stmt);
		}		
		
		if (o instanceof ShowIndexStmt) {
			ShowIndexStmt stmt = (ShowIndexStmt) o;
			dbQuery = new ShowIndexQuery(stmt);
		}
		
		if (o instanceof ShowSchemaStmt) {
			dbQuery = new ShowSchemaQuery();
		}
		
		if (query.startsWith("explain plan")) {
			dbQuery = new DBShowPlanQuery(query, bindParamList);
		}
		
		if (o instanceof CreateStorageStmt) {
			CreateStorageStmt stmt = (CreateStorageStmt) o;
			dbQuery = new DBCreateStorageQuery(query, stmt);
		}
		
		if (o instanceof CreateShardStmt) {
			CreateShardStmt stmt = (CreateShardStmt) o;
			dbQuery = new CreateShardQuery(query, stmt);
		}
		
		if (o instanceof CreateReplicaSetStmt) {
			CreateReplicaSetStmt stmt = (CreateReplicaSetStmt) o;
			dbQuery = new CreateReplicaSetQuery(query, stmt);
		}
		
		if (o instanceof AddToReplicaSetStmt) {
			AddToReplicaSetStmt stmt = (AddToReplicaSetStmt) o;
			dbQuery = new AddToReplicaSetQuery(query, stmt);
		}
		
		return dbQuery;
	}
}
