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
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.parser.UQLParser.InsertStmt;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.StringType;



public class DBInsertQuery extends BaseDBQuery {
	InsertStmt stmt;
	Map<String, DBType> map = new HashMap<String, DBType>();
	String collectionName;
	List<DBType> bindParamList = new ArrayList<DBType>();
	int currentBindPosn = 0;
	AndExpression andExp = null;
	static long count = -1;
	
	public DBInsertQuery(String query, InsertStmt stmt, List<DBType> bindParamList, int type, ChannelBuffer buffer) {
		super(query, type, buffer);
		this.stmt = stmt;
		collectionName = stmt.table;
		this.bindParamList = bindParamList;
		buildMap();
	}
	
	public String getCollenctionName() {
		return collectionName;
	}
	
	public Map<String, DBType> getInsertMap() {
		return map;
	}
	
	public AndExpression getExpression() {
		return null;
	}
	
	public List<DBType> getBindParamList() {
		return bindParamList;
	}
	
	private void buildMap() {
		for (int i = 0; i < stmt.columns.size(); i++) {
			String colName = stmt.columns.get(i);
			DBType type = getValue(stmt.values.list.get(i));
			map.put(colName, type);
		}
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public int execute() throws InvalidCollectionNameException {
//		SelectStmt s = stmt.values.stmt;
//		List<Map<CollectionAlias, RecordId>> list = null;
//		if (s != null) {
//			DBSelectQuery q = new DBSelectQuery(s, bindParamList);
//			list = q.executeGetDC();
//			for (int i = 0; i < list.size(); i++) {
//				Map<CollectionAlias, RecordId> map = list.get(i);
//				
//			}
//		}
//		
//		return TableRecordManager.getInstance().addTableRecord(collectionName, map);
		ScatterGatherQueryExecutor sgqe = new ScatterGatherQueryExecutor(this);
		return sgqe.insertQuery();
	}
	
	private StringType getValue(String s) {
		if (s == null) {
			return new StringType(null);
		}
		s = s.trim();
		if ("?".equals(s)) {
			DBType value = bindParamList.get(currentBindPosn++);
			if (value == null) {
				return null;
			}
			return new StringType(value.toString());
		}
		if (s.startsWith("'")) {
			s = s.replaceFirst("'", "");
		}
		if (s.substring(s.length()-1).equals("'")) {
			s = s.substring(0, s.length()-1);
		}
		s = s.replaceAll("\'", "'");
		return new StringType(s);
	}	
}
