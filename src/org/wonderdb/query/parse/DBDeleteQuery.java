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
import org.wonderdb.parser.TableDef;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.RecordId;


public class DBDeleteQuery extends BaseDBQuery {
	SimpleNode deleteNode = null;
	List<CollectionAlias> caList = null;
	Map<String, CollectionAlias> fromMap = null;
	SimpleNode filterNode = null;
	DBSelectQueryJTree selQuery = null;
	AndExpression andExp = null;
	
	public DBDeleteQuery(String q, SimpleNode query, int type, ChannelBuffer buffer) {
		super(q, query, type, buffer);
		this.deleteNode = query;

		SimpleNode tableNode = SimpleNodeHelper.getInstance().getFirstNode(deleteNode, UQLParserTreeConstants.JJTTABLEDEF);
		List<TableDef> tDefList = new ArrayList<TableDef>();
		TableDef tDef = SimpleNodeHelper.getInstance().getTableDef(tableNode);
		tDefList.add(tDef);
		caList = getCaList(tDefList);
		fromMap = getFromMap(caList);
		
		filterNode = SimpleNodeHelper.getInstance().getFirstNode(deleteNode, UQLParserTreeConstants.JJTFILTEREXPRESSION);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(getCollection());
		List<ColumnNameMeta> selectColumns = colMeta.getCollectionColumns();
		CollectionAlias ca = caList.get(0);
		List<Integer> columnIdList = new ArrayList<Integer>(selectColumns.size());
		Map<CollectionAlias, List<Integer>> selectColumnList = new HashMap<CollectionAlias, List<Integer>>();
		selectColumnList.put(ca, columnIdList);
		for (int i = 0; i < selectColumns.size(); i++) {
			ColumnNameMeta cnm = selectColumns.get(i);
			columnIdList.add(cnm.getCoulmnId());
		}
		selQuery = new DBSelectQueryJTree(getQueryString(), query, fromMap, selectColumnList, filterNode, type, buffer);
		andExp = selQuery.getAndExpression();
	}
	
	public String getCollection() {
		return caList.get(0).getCollectionName();
	}
	
	private List<CollectionAlias> getCaList(List<TableDef> list) {
		List<CollectionAlias> retList = new ArrayList<CollectionAlias>();
		for (int i = 0; i < list.size(); i++) {
			TableDef tDef = list.get(i);
			CollectionAlias ca = new CollectionAlias(tDef.table, tDef.alias);
			retList.add(ca);
		}
		
		return retList;
	}
	
	private Map<String, CollectionAlias> getFromMap(List<CollectionAlias> caList) {
		Map<String, CollectionAlias> fromMap = new HashMap<String, CollectionAlias>();
		for (int i = 0; i < caList.size(); i++) {
			CollectionAlias ca = caList.get(i);
			fromMap.put(ca.getAlias(), ca);
		}
		return fromMap;
	}
	

	public int execute(Shard shard) {
		List<Map<CollectionAlias, RecordId>> recListList = selQuery.executeGetDC(shard);
		int count = 0;
		for (int i = 0; i < recListList.size(); i++) {
			Map<CollectionAlias, RecordId> recList = recListList.get(i);
			if (recList.size() == 1) {
				Iterator<CollectionAlias> iter = recList.keySet().iterator();
				while (iter.hasNext()) {
					CollectionAlias ca = iter.next();
					RecordId recId = recList.get(ca);
					count = count + TableRecordManager.getInstance().delete(getCollection(), shard, recId, filterNode, fromMap);
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
