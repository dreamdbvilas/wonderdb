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
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.parser.UQLParser.DeleteStmt;
import org.wonderdb.parser.UQLParser.SelectStmt;
import org.wonderdb.parser.UQLParser.TableDef;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.types.DBType;

public class DBDeleteQuery extends BaseDBQuery {
	DeleteStmt stmt = null;
	List<DBType> bindParamList = null;
	DBSelectQuery selQuery = null;
	AndExpression andExp = null;
	
	public DBDeleteQuery(String query, DeleteStmt stmt, List<DBType> bindParamList, int type, ChannelBuffer buffer) {
		super(query, type, buffer);
		this.stmt = stmt;
		this.bindParamList = bindParamList;

		SelectStmt sStmt = new SelectStmt();
		TableDef tDef = new TableDef();
		tDef.alias = "";
		tDef.table = stmt.table;
		List<TableDef> tDefList = new ArrayList<TableDef>();
		tDefList.add(tDef);
		sStmt.tableDefList = tDefList;
		sStmt.filter = stmt.filter;
		List<String> list = new ArrayList<String>();
		list.add("*");
		sStmt.selectColList = list;
		selQuery = new DBSelectQuery(query, sStmt, bindParamList, type, buffer);
		andExp = new AndExpression(selQuery.expList);
	}
	
	public String getCollection() {
		return stmt.table;
	}
	
	
	public int execute(Shard shard) {
		List<Map<CollectionAlias, RecordId>> recListList = selQuery.executeGetDC(shard);
		List<BasicExpression> expList = selQuery.getExpList();
		int count = 0;
		for (int i = 0; i < recListList.size(); i++) {
			Map<CollectionAlias, RecordId> recList = recListList.get(i);
			if (recList.size() == 1) {
				Iterator<CollectionAlias> iter = recList.keySet().iterator();
				while (iter.hasNext()) {
					CollectionAlias ca = iter.next();
					RecordId recId = recList.get(ca);
					count = count + TableRecordManager.getInstance().delete(stmt.table, shard, recId, expList);
				}
			}
		}
		return count;
	}
	
	@Override
	public AndExpression getExpression() {
		return andExp;
	}
}
