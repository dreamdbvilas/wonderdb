package org.wonderdb.query.parser.jtree;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.parser.jtree.QueryEvaluator;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.Token;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.query.parse.BaseDBQuery;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.plan.AndQueryExecutor;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.query.plan.QueryPlan;
import org.wonderdb.query.plan.QueryPlanBuilder;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.schema.WonderDBFunction;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.DBType;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.record.RecordManager;
import org.wonderdb.types.record.RecordManager.BlockAndRecord;
import org.wonderdb.types.record.TableRecord;




public class DBSelectQueryJTree extends BaseDBQuery {
	protected Map<CollectionAlias, List<Integer>> selectColumnNames = new HashMap<CollectionAlias, List<Integer>>();
	List<SimpleNode> selectStmt = null;
	Map<String, CollectionAlias> fromMap = new HashMap<String, CollectionAlias>();
	int type = -1;
	protected Set<Object> pinnedBlocks = new HashSet<Object>();
	public List<ResultSetColumn> resultsetColumns = new ArrayList<ResultSetColumn>();
	AndExpression andExp = null;

	public DBSelectQueryJTree(String q, SimpleNode query, SimpleNode selectStmt, int type, ChannelBuffer buffer) {
		super(q, query, type, buffer);
		QueryEvaluator qe = new QueryEvaluator(null, null);
		this.selectStmt = qe.getSelectStmts(selectStmt);
		fromMap = SimpleNodeHelper.getInstance().getFromMap(selectStmt);
		this.type = type;
		buildSelectColumnNames(fromMap);
		buildResultSetColumnList();
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(selectStmt, UQLParserTreeConstants.JJTFILTEREXPRESSION);
		node = SimpleNodeHelper.getInstance().getFirstNode(node, UQLParserTreeConstants.JJTCOMPAREEQUATION);
		if (node != null) {
			List<BasicExpression> andList = SimpleNodeHelper.getInstance().buildAndExpressionList(node, new ArrayList<>(fromMap.values()));
			andExp = new AndExpression(andList);
		}
	}
	
	public DBSelectQueryJTree(String q, SimpleNode query, Map<String, CollectionAlias> fromMap, Map<CollectionAlias, List<Integer>> selectColumnNames, 
			SimpleNode filterNode, int type, ChannelBuffer buffer ) {
		super(q, query, type, buffer);
		this.fromMap = fromMap;
		this.type = type;
		this.selectColumnNames = selectColumnNames;

		SimpleNode node = filterNode;
		node = SimpleNodeHelper.getInstance().getFirstNode(node, UQLParserTreeConstants.JJTCOMPAREEQUATION);
		if (node != null) {
			List<BasicExpression> andList = SimpleNodeHelper.getInstance().buildAndExpressionList(node, new ArrayList<>(fromMap.values()));
			andExp = new AndExpression(andList);
		}
		selectStmt = new ArrayList<>();
		selectStmt.add(query);
}
	
	public List<CollectionAlias> getFromList() {
		return new ArrayList<>(fromMap.values());
	}
	
//	public String getQuery() {
//		return query;
//	}
//	
	public AndExpression getAndExpression() {
		return andExp;
	}
	
	public List<QueryPlan> getPlan(Shard shard) {
		List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(new ArrayList<>(fromMap.values()), shard, andExp, pinnedBlocks);
		return planList;
	}
	
	public List<List<ResultSetValue>> execute() {
		if (selectStmt.size() == 1) {
			ScatterGatherQueryExecutor sgqe = new ScatterGatherQueryExecutor(this);
			return sgqe.selectQuery();
		} 
		List<List<ResultSetValue>> retVal = new ArrayList<List<ResultSetValue>>();
		for (int i = 0; i < selectStmt.size(); i++) {
			DBSelectQueryJTree selectQ = new DBSelectQueryJTree(getQueryString(), null, selectStmt.get(i), type, buffer);
			ScatterGatherQueryExecutor sgqe = new ScatterGatherQueryExecutor(selectQ);
			List<List<ResultSetValue>> l = sgqe.selectQuery();
			retVal.addAll(l);
			
		}
		return retVal;
	}
	
	public List<List<ResultSetValue>> executeLocal(Shard shard) {
		List<List<ResultSetValue>> resultListList = new ArrayList<List<ResultSetValue>>();
		try {
			List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(new ArrayList<>(fromMap.values()), shard, andExp, pinnedBlocks);
			
			List<Map<CollectionAlias, RecordId>> mapList = new ArrayList<Map<CollectionAlias,RecordId>>();
			AndQueryExecutor.getInstance().executeTree(shard, planList, selectStmt.get(0), false, new ArrayList<>(fromMap.values()), fromMap, 
					selectColumnNames, mapList, null, true);
			DataContext dc = new DataContext();
			List<ResultSetValue> resultList = new ArrayList<ResultSetValue>();
			boolean isAgegate = false;
			for (int i = 0; i < mapList.size(); i++) {
				Map<CollectionAlias, RecordId> map = mapList.get(i);
				Iterator<CollectionAlias> iter = map.keySet().iterator();
				Set<Object> pinnedBlocks = new HashSet<Object>();
				dc = new DataContext(dc);
				while (iter.hasNext()) {
					CollectionAlias ca = iter.next();
					RecordId recId = map.get(ca);
					TableRecordMetadata meta = (TableRecordMetadata) SchemaMetadata.getInstance().getTypeMetadata(ca.getCollectionName());
					List<Integer> selectCols = selectColumnNames.get(ca);
					BlockAndRecord bar = null;
					try {
						bar = RecordManager.getInstance().getTableRecordAndLock(recId, selectCols, meta, pinnedBlocks);
						if (bar == null || bar.block == null || bar.record == null) {
							continue;
						}
						dc.add(ca, new StaticTableResultContent((TableRecord) bar.record)); 
					} finally {
						if (bar != null && bar.block != null) {
							CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
						}
					}
				}

				if (AndQueryExecutor.filterTree(dc, selectStmt.get(0), fromMap)) {
					if (!isAgegate) {
						resultList = new ArrayList<ResultSetValue>();
						resultListList.add(resultList);
					} else {
						resultList.clear();
					}
					for (int x = 0; x < resultsetColumns.size(); x++) {
						ResultSetColumn rsc = resultsetColumns.get(x);
						ResultSetValue rsv = new ResultSetValue();
						if (rsc instanceof VirtualResultSetColumn) {
							dc.processFunction(((VirtualResultSetColumn) rsc).function);
							if (!isAgegate) {
								isAgegate = ((VirtualResultSetColumn) rsc).function.isAggregate();
							}
							rsv.value = dc.getGlobalValue(((VirtualResultSetColumn) rsc).function.getColumnType());
							rsv.columnName = ((VirtualResultSetColumn) rsc).columnAlias;
						} else {
							rsv.value = dc.getResultMap().get(rsc.ca).getValue(rsc.columnId);
							rsv.columnName = rsc.resultColumnName;
						}
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
//		List<Map<CollectionAlias, RecordId>> resultList = new ArrayList<Map<CollectionAlias,RecordId>>();
		List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(new ArrayList<>(fromMap.values()), shard, andExp, pinnedBlocks);
		List<Map<CollectionAlias, RecordId>> mapList = new ArrayList<Map<CollectionAlias,RecordId>>();
		AndQueryExecutor.getInstance().executeTree(shard, planList, selectStmt.get(0), false, new ArrayList<>(fromMap.values()), fromMap, selectColumnNames, mapList, null, true);

		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);

		for (Map<CollectionAlias, RecordId> map : mapList) {
			for (CollectionAlias ca : map.keySet()) {
				Map<CollectionAlias, TableRecord> m1 = new HashMap<CollectionAlias, TableRecord>();
				retList.add(m1);
				TableRecordMetadata meta = (TableRecordMetadata) SchemaMetadata.getInstance().getTypeMetadata(ca.getCollectionName());
				RecordId recId = map.get(ca);
				List<Integer> selectCols = selectColumnNames.get(ca);
				
				BlockAndRecord bar = null;
				try {
					bar = RecordManager.getInstance().getTableRecordAndLock(recId, selectCols, meta, pinnedBlocks);
					if (bar == null || bar.block == null || bar.record == null) {
						continue;
					}
					m1.put(ca, (TableRecord) bar.record);
				} finally {
					CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
				}
			}
		}
		return retList;
	}
	
	public List<Map<CollectionAlias, RecordId>> executeGetDC(Shard shard) {
		List<Map<CollectionAlias, RecordId>> resultList = new ArrayList<Map<CollectionAlias,RecordId>>();
		try {
			List<QueryPlan> planList = QueryPlanBuilder.getInstance().build(new ArrayList<>(fromMap.values()), shard, andExp, pinnedBlocks);
			AndQueryExecutor.getInstance().executeTree(shard, planList, selectStmt.get(0), false, new ArrayList<>(fromMap.values()), fromMap, selectColumnNames, resultList, null, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			pinnedBlocks.clear();
		}
		return resultList;		
	}
		
	private void buildSelectColumnNames(Map<String, CollectionAlias> map) {
		List<SimpleNode> columnAliasList = new ArrayList<>();
		SimpleNodeHelper.getInstance().getNodes(queryNode, UQLParserTreeConstants.JJTCOLUMNANDALIAS, columnAliasList);
		buildSelectColumnNames(columnAliasList);
	}
	
	private void buildResultSetColumnList() {
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(queryNode, UQLParserTreeConstants.JJTLITERALLIST);
		buildResultSetColumnList(node);
	}
	
	private void buildResultSetColumnList(SimpleNode literalListNode) {
		List<SimpleNode> list = new ArrayList<>(); 
		SimpleNodeHelper.getInstance().getNodes(literalListNode, UQLParserTreeConstants.JJTCOLUMNANDALIAS, list);
		for (int i = 0; i < list.size(); i++) {
			SimpleNode node = list.get(i);
			Token first = node.jjtGetFirstToken();
			Token last = node.jjtGetLastToken();
			String columnName = last.image;
			String alias = "";
			if (first != last) {
				alias = first.image;
			}
			if ("*".equals(columnName)) {
				buildAllResultSetColumns();
				return;
			}
			CollectionAlias ca = fromMap.get(alias);
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
			int colId = colMeta.getColumnId(columnName);
			ResultSetColumn rsc = new ResultSetColumn();
			rsc.ca = ca;
			rsc.columnId = colId;
			rsc.resultColumnName = colMeta.getColumnName(colId);
			resultsetColumns.add(rsc);
		}
	}
	
	private void buildAllResultSetColumns() {
		Iterator<CollectionAlias> iter = fromMap.values().iterator();
		while (iter.hasNext()) {
			CollectionAlias ca = iter.next();
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
			List<ColumnNameMeta> list = colMeta.getCollectionColumns();
			for (int i = 0; i < list.size(); i++) {
				ColumnNameMeta cnm = list.get(i);
				ResultSetColumn rsc = new ResultSetColumn();
				rsc.ca = ca;
				rsc.columnId = cnm.getCoulmnId();
				rsc.resultColumnName = cnm.getColumnName();
				resultsetColumns.add(rsc);
			}
		}		
	}
	
	private void buildSelectColumnNames(List<SimpleNode> list) {
		for (int i = 0; i < list.size(); i++) {
			SimpleNode node = list.get(i);
			Token first = node.jjtGetFirstToken();
			Token last = node.jjtGetLastToken();
			String columnName = last.image;
			String alias = "";
			if (first != last) {
				alias = first.image;
			}
			if ("*".equals(columnName)) {
				buildAllSelectColumns();
				return;
			}
			CollectionAlias ca = fromMap.get(alias);
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
			int colId = colMeta.getColumnId(columnName);
			List<Integer> colIdList = selectColumnNames.get(ca);
			if (colIdList == null) {
				colIdList = new ArrayList<>();
				selectColumnNames.put(ca, colIdList);
			}
			colIdList.add(colId);
		}
	}
	
	private void buildAllSelectColumns() {
		Iterator<CollectionAlias> iter = fromMap.values().iterator();
		while (iter.hasNext()) {
			CollectionAlias ca = iter.next();
			List<Integer> colIdList = selectColumnNames.get(ca);
			if (colIdList == null) {
				colIdList = new ArrayList<>();
				selectColumnNames.put(ca, colIdList);
			}
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
			List<ColumnNameMeta> list = colMeta.getCollectionColumns();
			for (int i = 0; i < list.size(); i++) {
				ColumnNameMeta cnm = list.get(i);
				colIdList.add(cnm.getCoulmnId());
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

	@Override
	public AndExpression getExpression() {
		return andExp;
	}
	
	public static class ResultSetColumn {
		public CollectionAlias ca;
		public int columnId;
		public String path;
		public String resultColumnName;
	}
	
	public static class VirtualResultSetColumn extends ResultSetColumn {
		public String columnAlias;
		public SimpleNode node = null;
		public WonderDBFunction function = null;
	}
	
	public static class ResultSetValue {
		public CollectionAlias ca;
		public String columnName;
		public DBType value;
	}	
	

}
