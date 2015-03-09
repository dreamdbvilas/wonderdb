package org.wonderdb.parser.jtree;

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

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.query.parse.CreateIndexQuery;
import org.wonderdb.query.parse.DBCreateStorageQuery;
import org.wonderdb.query.parse.DBCreateTableQuery;
import org.wonderdb.query.parse.DBDeleteQuery;
import org.wonderdb.query.parse.DBInsertQuery;
import org.wonderdb.query.parse.DBQuery;
import org.wonderdb.query.parse.DBShowPlanQuery;
import org.wonderdb.query.parse.DBUpdateQuery;
import org.wonderdb.query.parse.ShowSchemaQuery;
import org.wonderdb.query.parse.ShowTableQuery;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree;
import org.wonderdb.query.plan.DataContext;




public class QueryParser {
	private static QueryParser parser = new QueryParser();
	private QueryParser() {
	}
	public static QueryParser getInstance() {
		return parser;
	}

	public DBQuery parse(String query) {
		return parse(query, new ArrayList<>(), -1, null);
	}
	
	public DBQuery parse(String query, List<Object> bindParamList, int type, ChannelBuffer buffer) {
		UQLParser parser = new UQLParser(query);
		SimpleNode node = null;
		try {
			node = parser.Start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		List<SimpleNode> bindList = new ArrayList<>();
		SimpleNodeHelper.getInstance().getNodes(node, UQLParserTreeConstants.JJTQ, bindList);
		if (bindList.size() != bindParamList.size()) {
			throw new RuntimeException("Invalid bind params");
		}
		
		for (int i = 0; i < bindParamList.size(); i++) {
			Object o = bindParamList.get(i);
			SimpleNode n = bindList.get(i);
			n.jjtSetValue(o);
		}
		
		DBQuery dbQuery = null;
		node = (SimpleNode) node.children[0];
		switch (node.id) {
			case UQLParserTreeConstants.JJTSELECTSTMT:
				DBSelectQueryJTree selectTreeQuery = new DBSelectQueryJTree(query, node, node, type, buffer);
				return selectTreeQuery;			
			case UQLParserTreeConstants.JJTINSERTSTMT:
				dbQuery = new DBInsertQuery(query, node, type, new DataContext(), buffer);
				return dbQuery;
			case UQLParserTreeConstants.JJTCREATEINDEX:
				dbQuery = new CreateIndexQuery(query, node);
				return dbQuery;
			case UQLParserTreeConstants.JJTCREATETABLE:
				dbQuery = new DBCreateTableQuery(query, node);
				return dbQuery;
			case UQLParserTreeConstants.JJTUPDATESTMT:
				dbQuery = new DBUpdateQuery(query, node, type, buffer);
				return dbQuery;
			case UQLParserTreeConstants.JJTDELETESTMT:
				dbQuery = new DBDeleteQuery(query, node, type, buffer);
				return dbQuery;
			case UQLParserTreeConstants.JJTSHOWTABLE:
				dbQuery = new ShowTableQuery(query, node);
				return dbQuery;
//			case UQLParserTreeConstants.JJTSHOWINDEX:
//				dbQuery = new ShowIndexQuery(stmt);
//				return dbQuery;
			case UQLParserTreeConstants.JJTSHOWSCHEMA: 
				dbQuery = new ShowSchemaQuery(query);
				return dbQuery;
			case UQLParserTreeConstants.JJTEXPLAINPLAN:
				dbQuery = new DBShowPlanQuery(query, node);
				return dbQuery;
			case UQLParserTreeConstants.JJTCREATESTORAGE:
				dbQuery = new DBCreateStorageQuery(query, node);
				return dbQuery;
			default:
				throw new RuntimeException("Invalid query");
		}
	}
}
