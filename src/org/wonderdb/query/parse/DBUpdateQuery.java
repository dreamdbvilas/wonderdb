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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.parser.UQLParser.SelectStmt;
import org.wonderdb.parser.UQLParser.TableDef;
import org.wonderdb.parser.UQLParser.UpdateSetExpression;
import org.wonderdb.parser.UQLParser.UpdateStmt;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.StringType;


public class DBUpdateQuery extends BaseDBQuery {
	UpdateStmt stmt = null;
	List<DBType> bindParamList = null;
	AndExpression andExp = null;
	DBSelectQuery selQuery = null;
	
	public DBUpdateQuery(String query, UpdateStmt stmt, List<DBType> bindParamList, int type, ChannelBuffer buffer) {
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
		Map<String, DBType> changedColumns = new HashMap<String, DBType>();
		List<UpdateSetExpression> updateList = stmt.updateSetExpList;
		List<DBType> selectBindParam = new ArrayList<DBType>(bindParamList);
		for (int i = 0; i < updateList.size(); i++) {
			if ("?".equals(updateList.get(i).value)) {
				selectBindParam.remove(0);
			}
		}
		sStmt.selectColList = new ArrayList<String>();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(stmt.table);
//		boolean setColumnValue = false;
		sStmt.selectColList.add("objectId");
		for (int i = 0; i < updateList.size(); i++) {
			UpdateSetExpression exp = updateList.get(i);
			String colName = exp.column;
			sStmt.selectColList.add(colName);
			int colId = colMeta.getColumnId(exp.value);
			if (colId >= 0) {
				sStmt.selectColList.add(exp.value);
				CollectionColumn cc = colMeta.getCollectionColumn(colId);
				changedColumns.put(colName, cc);
//				setColumnValue = true;
			} else {
				changedColumns.put(colName, getValue(exp.value));				
			}
		}
		selQuery = new DBSelectQuery(query, sStmt, selectBindParam, type, buffer);
		andExp = new AndExpression(selQuery.expList);
	}
	
	public AndExpression getExpression() {
		return andExp;
	}
	
	public String getCollectionName() {
		return stmt.table;
	}
	
	public int execute(Shard shard) {
//		int[] shards = DefaultClusterManager.getInstance().getShards(andExp);
//		if (type == DreamDBShardServerHandler.SERVER_HANDLER) {
//			for (int i = 0; i < shards.length; i++) {
//				int shardId = shards[i];
//				if (shardId == DefaultClusterManager.getInstance().getMachineId()) {
//					// we should process
//				} else {
//					Channel channel = DefaultClusterManager.getInstance().getMaster(shardId);
//					// process with connection
//				}
//			}
//		} else {
//			// we just process but dont do scatter gather.
//		}

//		selQuery.selectColumnNames = new HashMap<CollectionAlias, List<ColumnType>>();
//		selQuery.selectColumnNames.put(ca, new ArrayList<ColumnType>(changedColumns.keySet()));

		
		SelectStmt sStmt = new SelectStmt();
		TableDef tDef = new TableDef();
		tDef.alias = "";
		tDef.table = stmt.table;
		List<TableDef> tDefList = new ArrayList<TableDef>();
		tDefList.add(tDef);
		sStmt.tableDefList = tDefList;
		sStmt.filter = stmt.filter;
		Map<String, DBType> changedColumns = new HashMap<String, DBType>();
		List<UpdateSetExpression> updateList = stmt.updateSetExpList;
		List<DBType> selectBindParam = new ArrayList<DBType>(bindParamList);
		for (int i = 0; i < updateList.size(); i++) {
			if ("?".equals(updateList.get(i).value)) {
				selectBindParam.remove(0);
			}
		}
		sStmt.selectColList = new ArrayList<String>();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(stmt.table);
		boolean setColumnValue = false;
//		sStmt.selectColList.add("objectId");
		for (int i = 0; i < updateList.size(); i++) {
			UpdateSetExpression exp = updateList.get(i);
			String colName = exp.column;
			sStmt.selectColList.add(colName);
			int colId = colMeta.getColumnId(exp.value);
			if (colId >= 0) {
				sStmt.selectColList.add(exp.value);
				CollectionColumn cc = colMeta.getCollectionColumn(colId);
				changedColumns.put(colName, cc);
				setColumnValue = true;
			} else {
				changedColumns.put(colName, getValue(exp.value));				
			}
		}
		
		selQuery = new DBSelectQuery(query, sStmt, selectBindParam, type, buffer);
		andExp = new AndExpression(selQuery.expList);

		List<Map<CollectionAlias, RecordId>> recListList = selQuery.executeGetDC(shard);
		List<BasicExpression> expList = selQuery.getExpList();

		int updateCount = 0;

		int m = 0;
		for (int x = 0; x < updateList.size(); x++) {
			if ("?".equals(updateList.get(x).value)) {
				changedColumns.put(updateList.get(x).column, bindParamList.get(m++));
			}
		}
		
		Map<String, DBType> tmpChangedColumns = new HashMap<String, DBType>(changedColumns);
		
		for (int i = 0; i < recListList.size(); i++) {
			Map<CollectionAlias, RecordId> recList = recListList.get(i);
			if (recList.size() == 1) {
				Iterator<CollectionAlias> iter = recList.keySet().iterator();
				while (iter.hasNext()) {
					CollectionAlias ca = iter.next();
					RecordId recId = recList.get(ca);
					TransactionId txnId = null;
					try {
						if (setColumnValue) {
							tmpChangedColumns = new HashMap<String, DBType>(changedColumns);
						}
						if (colMeta.isLoggingEnabled()) {
							txnId = LogManager.getInstance().startTxn();
						}
						updateCount = updateCount + TableRecordManager.getInstance().updateTableRecord(stmt.table, tmpChangedColumns, shard, recId, expList, txnId);
					} catch (Exception e) {
						throw new RuntimeException(e);
					} finally {
						LogManager.getInstance().commitTxn(txnId);
					}
				}
			}
		}
		return updateCount;
	}
	
	private StringType getValue(String val) {
		if (val == null) {
			return new StringType(null);
		}
		
		if (val.startsWith("'")) {
			val = val.substring(1);
		}
		
		if (val.endsWith("'")) {
			val = val.substring(0, val.length()-1);
		}
		
		return new StringType(val);
	}
}
