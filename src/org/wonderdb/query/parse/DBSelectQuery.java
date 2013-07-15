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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.ObjectId;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.BindStaticOperand;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.StaticOperand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.parser.UQLParser.Expression;
import org.wonderdb.parser.UQLParser.SelectStmt;
import org.wonderdb.parser.UQLParser.TableDef;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.query.plan.AndQueryExecutor;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.query.plan.QueryPlan;
import org.wonderdb.query.plan.QueryPlanBuilder;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.DoubleType;
import org.wonderdb.types.impl.FloatType;
import org.wonderdb.types.impl.IntType;
import org.wonderdb.types.impl.LongType;
import org.wonderdb.types.impl.StringType;



public class DBSelectQuery extends BaseDBQuery {
	SelectStmt stmt = null;
	Map<CollectionAlias, List<ColumnType>> selectColumnNames = new HashMap<CollectionAlias, List<ColumnType>>();
	public List<ResultSetColumn> resultsetColumns = new ArrayList<DBSelectQuery.ResultSetColumn>();
//	Map<CollectionAlias, List<ColumnType>> allSelectColumns = new HashMap<CollectionAlias, List<ColumnType>>();
	List<DBType> bindParamList = null;
	List<CollectionAlias> fromList = new ArrayList<CollectionAlias>();
	List<BasicExpression> expList = new ArrayList<BasicExpression>();
	boolean selectAllColumns = false;
	Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
	List<BindStaticOperand> bindStaticOperands = new ArrayList<BindStaticOperand>();
	int bindPramPosn = 0;
	AndExpression andExp = null;
	
	public DBSelectQuery(String query, SelectStmt stmt, List<DBType> bindParamList, int type, ChannelBuffer buffer) {
		super(query, type, buffer);
		this.stmt = stmt;
		this.bindParamList = bindParamList;
		fromList = buildFromList();
		expList = buildExpList();
		buildSelectColumnNames();
		andExp = new AndExpression(expList);
	}
	 
	public AndExpression getExpression() {
		return andExp;
	}
	
	public List<DBType> getBindParamList() {
		return bindParamList;
	}
	
	public String getQuery() {
		return query;
	}
	
	public List<QueryPlan> getPlan(Shard shard) {
		List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(fromList, shard, andExp, pinnedBlocks);
		return planList;
	}
	
	public List<CollectionAlias> getFromList() {
		return fromList;
	}
	
//		List<List<ResultSetValue>> resultListList = new ArrayList<List<ResultSetValue>>();
	public List<List<ResultSetValue>> execute() {
//		
//		AndExpression andExp = new AndExpression(expList);
//		if (type == DreamDBShardServerHandler.SERVER_HANDLER) {
//			int[] shards = DefaultClusterManager.getInstance().getShards(andExp);
//			for (int i = 0; i < shards.length; i++) {
//				int shardId = shards[i];
//				if (shardId == DefaultClusterManager.getInstance().getMachineId()) {
//					// we should process
//				} else {
//					Channel channel = DefaultClusterManager.getInstance().getMaster(shardId);
//					channel.write(buffer);
//					// process with connection
//				}
//			}
//		} else if (type != DreamDBShardServerHandler.SHARD_HANDLER) {
//			throw new RuntimeException("Invalid type: " + type);
//		}		
		ScatterGatherQueryExecutor sgqe = new ScatterGatherQueryExecutor(this);
		return sgqe.selectQuery();
//		return resultListList;
	}
	
	public List<List<ResultSetValue>> executeLocal(Shard shard) {
		List<List<ResultSetValue>> resultListList = new ArrayList<List<ResultSetValue>>();
		try {
			AndExpression andExp = new AndExpression(expList);
			List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(fromList, shard, andExp, pinnedBlocks);
			
			List<Map<CollectionAlias, RecordId>> mapList = new ArrayList<Map<CollectionAlias,RecordId>>();
			AndQueryExecutor.getInstance().execute(shard, planList, expList, false, fromList, selectColumnNames, mapList, null, true);
	
			for (int i = 0; i < mapList.size(); i++) {
				Map<CollectionAlias, RecordId> map = mapList.get(i);
				Iterator<CollectionAlias> iter = map.keySet().iterator();
				Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
				DataContext dc = new DataContext();
				while (iter.hasNext()) {
					CollectionAlias ca = iter.next();
					RecordId recId = map.get(ca);
					int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName()).getSchemaId();
					RecordBlock rb = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recId.getPtr(), schemaId, pinnedBlocks);
					rb.readLock();
					try {
						List<ColumnType> selectCols = selectColumnNames.get(ca);
						if (selectCols.contains(new ColumnType("*"))) {
							selectCols = null;
						}
						QueriableBlockRecord qbr = CacheObjectMgr.getInstance().getRecord(rb, recId, selectCols, schemaId, pinnedBlocks);
						Map<ColumnType, DBType> m = qbr.copyAndGetColumns(selectCols);
						TableRecord tr = new TableRecord(m);
						dc.add(ca, new StaticTableResultContent(tr, recId, schemaId));
					} finally {
						rb.readUnlock();
						CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
					}
				}
			
				if (AndQueryExecutor.filter(dc, expList)) {
					List<ResultSetValue> resultList = new ArrayList<DBSelectQuery.ResultSetValue>();
					resultListList.add(resultList);
					for (int x = 0; x < resultsetColumns.size(); x++) {
						ResultSetColumn rsc = resultsetColumns.get(x);
						ResultSetValue rsv = new ResultSetValue();
						rsv.value = dc.getResultMap().get(rsc.ca).getValue(rsc.cc.getColumnType(), rsc.path);
						rsv.columnName = rsc.cc.getColumnName();
						rsv.ca = rsc.ca;
						resultList.add(rsv);
					}
				}			
			}
			
			return resultListList;
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);			
		}
	}

	
	public List<Map<CollectionAlias, TableRecord>> executeAndGetTableRecord(Shard shard) {
		List<Map<CollectionAlias, TableRecord>> retList = new ArrayList<Map<CollectionAlias,TableRecord>>();
		AndExpression andExp = new AndExpression(expList);
		List<Map<CollectionAlias, RecordId>> resultList = new ArrayList<Map<CollectionAlias,RecordId>>();
		List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(fromList, shard, andExp, pinnedBlocks);
		AndQueryExecutor.getInstance().execute(shard, planList, expList, false, fromList, selectColumnNames, resultList, null, true);
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);

		for (Map<CollectionAlias, RecordId> map : resultList) {
			for (CollectionAlias ca : map.keySet()) {
				Map<CollectionAlias, TableRecord> m1 = new HashMap<CollectionAlias, TableRecord>();
				retList.add(m1);
				int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName()).getSchemaId();
				RecordId recId = map.get(ca);
				RecordBlock rb = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(recId.getPtr(), schemaId, pinnedBlocks);
				try {
					rb.readLock();
					QueriableBlockRecord qbr = CacheObjectMgr.getInstance().getRecord(rb, recId, null, schemaId, pinnedBlocks);
					Map<ColumnType, DBType> m = qbr.copyAndGetColumns(null);
					TableRecord tr = new TableRecord(m);
					tr.setRecordId(recId);
					m1.put(ca, tr);
				} finally {
					rb.readUnlock();
				}
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
		return retList;
	}
	
	public List<Map<CollectionAlias, RecordId>> executeGetDC(Shard shard) {
		AndExpression andExp = new AndExpression(expList);
		List<Map<CollectionAlias, RecordId>> resultList = new ArrayList<Map<CollectionAlias,RecordId>>();
		try {
			List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(fromList, shard, andExp, pinnedBlocks);
			AndQueryExecutor.getInstance().execute(shard, planList, expList, false, fromList, selectColumnNames, resultList, null, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			pinnedBlocks.clear();
		}
//		filter(list);
		return resultList;		
	}
	
	public List<BasicExpression> getExpList() {
		return expList;
	}
	
//	private void filter(List<DataContext> list) {
//		Iterator<CollectionAlias> iter = null;
//		for (int x = 0; x < list.size(); x++) {
//			DataContext context = list.get(x);
//			iter = fromList.iterator();
//			Map<ColumnType, DBType> addColumns = new HashMap<ColumnType, DBType>();
//			
//			while (iter.hasNext()) {
//				CollectionAlias ca = iter.next();
//				TableRecord trt = null;
//				if (selectAllColumns) {
//					Map<ColumnType, DBType> colVals = context.geAllColumns(ca);
//					Iterator<ColumnType> iter1 = colVals.keySet().iterator();
//					while (iter1.hasNext()) {
//						ColumnType k = iter1.next();
//						DBType v = colVals.get(k);
//						addColumns.put(k, v);
//					}
//				} else {
////					iter = selectColumnNames.keySet().iterator();
//					List<ColumnType> l = selectColumnNames.get(ca);
//					for (int i = 0; i < l.size(); i++) {
//						ColumnType col = l.get(i);
//						if (col.getValue() instanceof String && "*".equals((String) col.getValue()) ) {
//							Map<ColumnType, DBType> colVals = context.geAllColumns(ca);
//							Iterator<ColumnType> iter1 = colVals.keySet().iterator();
//							while (iter1.hasNext()) {
//								ColumnType k = iter1.next();
//								DBType v = colVals.get(k);
//								addColumns.put(k, v);
//							}							
//						} else {
//							DBType val = context.getValue(ca, col);
//							addColumns.put(col, val);
//						}
//					}
//				}
//				trt = new TableRecord(addColumns);
////				context.addColumnd(ca, trt);
//			}
//			list.set(x, context);
//		}
//		
//	}

	private List<CollectionAlias> buildFromList() {
		List<CollectionAlias> list = new ArrayList<CollectionAlias>();
		List<TableDef> tableDefList = stmt.tableDefList;
		for (TableDef td : tableDefList) {
			if (td.alias == null) {
				td.alias="";
			}
			CollectionAlias ca = new CollectionAlias(td.table, td.alias);
			list.add(ca);
		}
		return list;
	}
	
	private void buildSelectColumnNames() {
//		Map<CollectionAlias, List<ColumnType>> map = new HashMap<CollectionAlias, List<ColumnType>>();
		List<String> list = stmt.selectColList;
		for ( String s : list) {
			String[] split = s.split("\\.");
			String alias = "";
			String colName = null;
			if (split.length == 2) {
				alias = split[0];
				colName = split[1];
			} else {
				colName = s;
			}
			
			colName = extractColumnName(colName);
			
			CollectionAlias ca = getCollectionAlias(alias);
			List<ColumnType> l = selectColumnNames.get(ca);
			if (l == null) {
				l = new ArrayList<ColumnType>();
				selectColumnNames.put(ca, l);
			}
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
			ResultSetColumn rsc = new ResultSetColumn();
			if (!"*".equals(colName)) {
				int id = colMeta.getColumnId(colName);
				if (id < 0) {
					String cName = extractColumnName(colName);
					id = colMeta.getColumnId(cName);
				}
				rsc.cc = colMeta.getCollectionColumn(id);
				rsc.ca = ca;
				if (s.startsWith("/") && s.endsWith("/")) {
					rsc.path = s.substring(1, s.length()-1);
				}
				resultsetColumns.add(rsc);
				if (rsc.cc == null) {
					rsc.cc = new CollectionColumn(colMeta, colName, StringType.SERIALIZER_NAME);
				}
				l.add(rsc.cc.getColumnType());
			} else {
				synchronized (colMeta) {
					List<CollectionColumn> l1 = colMeta.getCollectionColumns();
//					l.addAll(m.keySet());
					Iterator<CollectionColumn> iter = l1.iterator();
					while (iter.hasNext()) {
						rsc = new ResultSetColumn();
						CollectionColumn cc = iter.next();
						if (cc.isQueriable() || colMeta.getSchemaId() <= 2) {
							rsc.ca = ca;
							rsc.cc = cc;
							resultsetColumns.add(rsc);
							l.add(cc.getColumnType());
						}
					}
				}
			}
		}
		
		for (int i = 0; i < expList.size(); i++) {
			ColumnType allColumns = new ColumnType(new String("*"));
			BasicExpression exp = expList.get(i);
			if (exp.getLeftOperand() instanceof VariableOperand) {
				VariableOperand vo = (VariableOperand) exp.getLeftOperand();
				CollectionAlias alias = vo.getCollectionAlias();
				List<ColumnType> l = selectColumnNames.get(alias);
				if (!l.contains(vo.getColumnType())) {
					l.add(vo.getColumnType());
				}
			}
			if (exp.getRightOperand() instanceof VariableOperand) {
				VariableOperand vo = (VariableOperand) exp.getRightOperand();
				CollectionAlias alias = vo.getCollectionAlias();
				List<ColumnType> l = selectColumnNames.get(alias);
				if (!l.contains(vo.getColumnType()) && !l.contains(allColumns)) {
					l.add(vo.getColumnType());
				}
			}
		}
	}
	
	public static String extractColumnName(String cName) {
		String[] s = cName.split("://");
		if (s.length == 1) {
			return s[0];
		} 
		
		if (s.length == 2) {
			String x = s[1];
			String x1[] = x.split("/");
			String x2[] = x1[0].split(":");
			return x2[0];
			
		}
		return null;
	}

	public static String extractRootNode(String cName) {
		String[] s = cName.split("://");
		if (s.length == 1) {
			return s[0];
		} 
		
		if (s.length == 2) {
			String x = s[1];
			String x1[] = x.split("/");
			String x2[] = x1[0].split(":");
			
			if (x2.length == 2) {
				return x2[1];
			}
			
		}
		return null;
	}

	private CollectionAlias getCollectionAlias(String alias) {
		for (CollectionAlias ca : fromList) {
			if (alias.equals(ca.getAlias())) {
				return ca;
			}
		}
		return null;
	}
	
	private List<BasicExpression> buildExpList() {
		List<BasicExpression> list = new ArrayList<BasicExpression>();
		if (stmt.filter == null || stmt.filter.andList == null) {
			return list;
		}
		for (Expression exp : stmt.filter.andList) {
			String left = exp.leftOp;
			String op = exp.op;
			String right = exp.rightOp;
			Operand l = buildOperand(left);
			Operand r = buildOperand(right);
			
			if (l instanceof StaticOperand && r instanceof VariableOperand) {
				l = convertType((StaticOperand) l, (VariableOperand) r);
			} else if (l instanceof VariableOperand && r instanceof StaticOperand) {
				r = convertType((StaticOperand) r, (VariableOperand) l);
			}
			
			int operator = buildOp(op);
			BasicExpression e = new BasicExpression(l, r, operator);
			list.add(e);
		}
		return list;
	}
	
	private Operand convertType(StaticOperand so, VariableOperand vo) {
//		if (so instanceof BindStaticOperand) {
//			return so;
//		}
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(vo.getCollectionAlias().getCollectionName());
		String s = colMeta.getColumnSerializerName(vo.getColumnType());
		if ("ss".equals(s)) {
			return so;
		}
		StringType st = (StringType) so.getValue(null);
		String value = st == null ? null : st.get();
		if ("is".equals(s)) {
			if (value != null) {
				return new StaticOperand(new IntType(Integer.parseInt(value)));
			} else {
				return new StaticOperand(null);
			}
		}
		
		if ("ls".equals(s)) {
			if (value != null) {
				return new StaticOperand(new LongType(Long.parseLong(value)));
			} else {
				return new StaticOperand(null);
			}
		}
		
		if ("ds".equals(s)) {
			if (value != null) {
				return new StaticOperand(new DoubleType(Double.parseDouble(value)));
			} else {
				return new StaticOperand(null);
			}
		}
		
		if ("is".equals(s)) {
			if (value != null) {
				return new StaticOperand(new FloatType(Float.parseFloat(value)));
			} else {
				return new StaticOperand(null);
			}
		}
		return so;
	}
	
	
	private Operand buildOperand(String s) {
		Operand o = null;
		if (s==null) {
			return new StaticOperand(null);
		}
		if (s.startsWith("'") && s.endsWith("'")) {
			String str = s.substring(1, s.length()-1);
			o = new StaticOperand(new StringType(str));
		} else if ("?".equals(s)) {
			o = new BindStaticOperand(bindParamList.get(bindPramPosn++));
			bindStaticOperands.add((BindStaticOperand) o);
		} else {
			try {
				Double.parseDouble(s);
				o = new StaticOperand(new StringType(s)); 
			} catch (NumberFormatException e) {
				boolean virtualColumn = false;
				if (s.startsWith("/")) {
					s = s.substring(1);
					s = s.substring(0, s.length()-1);
					virtualColumn = true;
				}
				if (s.contains(".")) {
					String[] split = s.split("\\.");
					if (split.length == 2) {
						CollectionAlias ca = getCollectionAlias(split[0]);
						ColumnType ct = getColumnType(ca.getCollectionName(), split[1]);
						if (ct == null || ct.getValue() instanceof String) {
//							extractColumnName(cName)
							throw new RuntimeException("Invalid column: " + s);
						}
						
						o = new VariableOperand(getCollectionAlias(split[0]), ct, split[1]);
					}
				} else {
					ColumnType ct = getColumnType(fromList.get(0).getCollectionName(), s);
					if (ct == null || ct.getValue() instanceof String) {
						if (virtualColumn) {
							String colName = extractColumnName(s);
							ColumnType cxt = getColumnType(fromList.get(0).getCollectionName(), colName);
							if (cxt == null) {
								throw new RuntimeException("Invalid column: " + s);
							}
							o = new VariableOperand(fromList.get(0), cxt, s);
						} else {
							throw new RuntimeException("Invalid column: " + s);
						}
					} else {
						o = new VariableOperand(fromList.get(0), ct, null);
					}
				}
			}
		}
		return o;
	}
	
	private ColumnType getColumnType(String collectionName, String columnName) {
		CollectionMetadata colMetadata = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
//		String cName = extractColumnName(columnName);
		return colMetadata.getColumnType(columnName);
	}
	
	private int buildOp(String s) {
		if ("=".equals(s)) {
			return org.wonderdb.expression.Expression.EQ;
		}
		
		if (">".equals(s)) {
			return org.wonderdb.expression.Expression.GT;
		}
		
		if ("<".equals(s)) {
			return org.wonderdb.expression.Expression.LT;
		}
		
		if (">=".equals(s)) {
			return org.wonderdb.expression.Expression.GE;
		}
		
		if ("<=".equals(s)) {
			return org.wonderdb.expression.Expression.LE;
		}
		
		if ("<>".equals(s)) {
			return org.wonderdb.expression.Expression.NE;
		}
		
		if ("like".equals(s)) {
			return org.wonderdb.expression.Expression.LIKE;
		}
		
		throw new RuntimeException("Invalid operator");
	}
	
//	private void filter(DataContext dc, List<CollectionAlias> fromList,
//			Map<CollectionAlias, List<ColumnType>> selectColumnNames, Map<CollectionAlias, TableRecord> map) {
//		Iterator<CollectionAlias> iter = null;
//		DataContext context = dc;
//		iter = fromList.iterator();
//		Map<ColumnType, DBType> addColumns = new HashMap<ColumnType, DBType>();
//		
//		while (iter.hasNext()) {
//			CollectionAlias ca = iter.next();
//			List<ColumnType> l = selectColumnNames.get(ca);
//			addColumns = new HashMap<ColumnType, DBType>();
//			for (int i = 0; i < l.size(); i++) {
//				ColumnType col = l.get(i);
//				if (col.getValue() instanceof String && "*".equals((String) col.getValue()) ) {
//					Map<ColumnType, DBType> colVals;
//					colVals = context.getAllColumns(ca);
//					Iterator<ColumnType> iter1 = colVals.keySet().iterator();
//					while (iter1.hasNext()) {
//						ColumnType k = iter1.next();
//						DBType v = colVals.get(k);
//						addColumns.put(k, v);
//					}							
//				} else {
//					DBType val = context.getValue(ca, col);
//					addColumns.put(col, val);
//				}
//			}
//			TableRecord tr = new TableRecord(addColumns);
//			tr.setRecordId(context.get(ca));
//			map.put(ca, tr);
//		}
//	}

	public static class ResultSetColumn {
		public CollectionAlias ca;
		public CollectionColumn cc;
		public String path;
	}
	
	public static class ResultSetValue {
		public CollectionAlias ca;
		public String columnName;
		public DBType value;
	}	
	
	public static class RecordIdObjectId {
		private RecordId recordId = null;
		private ObjectId objectId = null;
		
		public RecordIdObjectId(RecordId recId, ObjectId objectId) {
			this.recordId = recId;
			this.objectId = objectId;
		}
		
		public RecordId getRecordId() {
			return recordId;
		}
		
		public ObjectId getObjectId() {
			return objectId;
		}
	}
//	
//	public class SelectExecuteTask implements Callable<List<List<ResultSetValue>>> {
//
//		@Override
//		public List<List<ResultSetValue>> call() throws Exception {
//			return executeLocal();
//		}		
//	}
}
