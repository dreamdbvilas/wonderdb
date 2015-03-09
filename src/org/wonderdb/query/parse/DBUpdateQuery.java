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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.parser.TableDef;
import org.wonderdb.parser.UpdateSetExpression;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.RecordId;



public class DBUpdateQuery extends BaseDBQuery {
	AndExpression andExp = null;
	SimpleNode filterNode = null;
	List<CollectionAlias> caList = null;
	Map<String, CollectionAlias> fromMap = null;
	List<UpdateSetExpression> updateSetExpList = null;
	DBSelectQueryJTree selQuery = null;
	
	public DBUpdateQuery(String q, SimpleNode query, int type, ChannelBuffer buffer) {
		super(q, query, type, buffer);

		SimpleNode tableNode = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTTABLEDEF);
		List<TableDef> tDefList = new ArrayList<TableDef>();
		TableDef tDef = SimpleNodeHelper.getInstance().getTableDef(tableNode);
		tDefList.add(tDef);
		caList = getCaList(tDefList);
		fromMap = getFromMap(caList);
		
		filterNode = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTFILTEREXPRESSION);
		
		List<SimpleNode> updateSetNodes = new ArrayList<>();
		SimpleNodeHelper.getInstance().getNodes(query, UQLParserTreeConstants.JJTUPDATECOLUMN, updateSetNodes);
		updateSetExpList = SimpleNodeHelper.getInstance().getUpdateSet(updateSetNodes, caList);
		
		List<SimpleNode> selectColumnNodeList = new ArrayList<>(); 
		Map<CollectionAlias, List<Integer>> selectColumnList = new HashMap<>();
		
		SimpleNodeHelper.getInstance().getNodes(query, UQLParserTreeConstants.JJTCOLUMNANDALIAS, selectColumnNodeList);
		for (int i = 0; i < selectColumnNodeList.size(); i++) {
			SimpleNode node = selectColumnNodeList.get(i);
			VariableOperand vo = SimpleNodeHelper.getInstance().getColumnId(node, caList);
			
			List<Integer> colList = selectColumnList.get(vo.getCollectionAlias());
			if (colList == null) {
				colList = new ArrayList<>();
				selectColumnList.put(vo.getCollectionAlias(), colList);
			}
			colList.add(vo.getColumnId());
		}

		selQuery = new DBSelectQueryJTree(getQueryString(), query, fromMap, selectColumnList, filterNode, type, buffer);
		andExp = selQuery.getAndExpression();
	}
	
	private List<CollectionAlias> getCaList(List<TableDef> list) {
		List<CollectionAlias> retList = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			TableDef tDef = list.get(i);
			CollectionAlias ca = new CollectionAlias(tDef.table, tDef.alias);
			retList.add(ca);
		}
		
		return retList;
	}
	
	private Map<String, CollectionAlias> getFromMap(List<CollectionAlias> caList) {
		Map<String, CollectionAlias> fromMap = new HashMap<>();
		for (int i = 0; i < caList.size(); i++) {
			CollectionAlias ca = caList.get(i);
			fromMap.put(ca.getAlias(), ca);
		}
		return fromMap;
	}
	
	public AndExpression getExpression() {
		return andExp;
	}
	
	public String getCollectionName() {
		return caList.get(0).getCollectionName();
	}
	
	public int execute(Shard shard) {
		List<Map<CollectionAlias, RecordId>> recListList = selQuery.executeGetDC(shard);

		int updateCount = 0;
		
		for (int i = 0; i < recListList.size(); i++) {
			Map<CollectionAlias, RecordId> recList = recListList.get(i);
			if (recList.size() == 1) {
				Iterator<CollectionAlias> iter = recList.keySet().iterator();
				while (iter.hasNext()) {
					CollectionAlias ca = iter.next();
					RecordId recId = recList.get(ca);
					TransactionId txnId = null;
					CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
					try {
						if (colMeta.isLoggingEnabled()) {
							txnId = LogManager.getInstance().startTxn();
						}
						updateCount = updateCount + TableRecordManager.getInstance().updateTableRecord(getCollectionName(), 
								recId, shard, updateSetExpList, queryNode, fromMap, txnId);
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
}
