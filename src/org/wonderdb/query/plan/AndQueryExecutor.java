package org.wonderdb.query.plan;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.parser.jtree.QueryEvaluator;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.record.RecordManager;
import org.wonderdb.types.record.RecordManager.BlockAndRecord;
import org.wonderdb.types.record.TableRecord;




public class AndQueryExecutor {
	private static AndQueryExecutor instance = new AndQueryExecutor();
	private AndQueryExecutor() {
	}
	
	public static AndQueryExecutor getInstance() {
		return instance;
	}
	
	public void executeTree(Shard shard, List<QueryPlan> plan, SimpleNode tree, boolean writeLock,
			List<CollectionAlias> fromList, Map<String, CollectionAlias> fromMap, Map<CollectionAlias, List<Integer>> selectColumnNames,
			List<Map<CollectionAlias, RecordId>> resultsList, List<Map<CollectionAlias, String>> slaveResultsList, boolean masterQuery) {
//		List<Map<CollectionAlias, RecordId>> results = new ArrayList<Map<CollectionAlias,RecordId>>();
		nestedLoop2(shard, plan, 0, null, tree, writeLock, fromList, fromMap, selectColumnNames, resultsList, slaveResultsList, masterQuery);
	}
	
	private void nestedLoop2(Shard shard, List<QueryPlan> plan, int posn, DataContext context, 
			SimpleNode tree, boolean writeLock,
			List<CollectionAlias> fromList, Map<String, CollectionAlias> fromMap, Map<CollectionAlias, List<Integer>> selectColumnNames,
			List<Map<CollectionAlias, RecordId>> resultsList, List<Map<CollectionAlias, String>> slaveResultsList, boolean masterQuery) {
	
		if (posn == 0) {
			context = new DataContext();
		}
		
		if (posn >= plan.size()) {
			return;
		}
		ResultIterator iter = null;
		try {
			iter = plan.get(posn).iterator(context, shard, selectColumnNames.get(posn), writeLock);
			String schemaObjectName = plan.get(posn).getCollectionAlias().getCollectionName();
			while (iter.hasNext()) {
				CollectionAlias ca = plan.get(posn).getCollectionAlias();
				ResultContent resultContent = (ResultContent) iter.next();
				context.add(ca, resultContent);
				boolean filter;
				filter = filterTree(context, tree, fromMap);
				if (filter) {
					if (resultContent instanceof IndexResultContent) {
						context.map.remove(ca);
						Set<Object> pinnedBlock = new HashSet<Object>();
						try {
							List<Integer> columnsToFetchFromTable = new ArrayList<Integer>();
							List<Integer> colIdList = selectColumnNames.get(ca);
							for (int i = 0; i < colIdList.size(); i++) {
								int colId = colIdList.get(i);
								columnsToFetchFromTable.add(colId);
//								if (!resultContent.getAllColumns().containsKey(colId)) {
//									columnsToFetchFromTable.add(colId);
//								}
							}
							BlockAndRecord bar = null;
							if (columnsToFetchFromTable.size() > 0) {
								TableRecordMetadata meta = (TableRecordMetadata) SchemaMetadata.getInstance().getTypeMetadata(schemaObjectName);
								bar = RecordManager.getInstance().getTableRecordAndLock(resultContent.getRecordId(), columnsToFetchFromTable, meta, pinnedBlock);
								if (bar == null || bar.block == null || bar.record == null) {
									continue;
								}
								resultContent = new StaticTableResultContent((TableRecord) bar.record);
							}
							context.map.put(ca, resultContent);
							filter = filterTree(context, tree, fromMap);
							if (!filter) {
								continue;
							}
						} finally {
							CacheEntryPinner.getInstance().unpin(pinnedBlock, pinnedBlock);
						}
					}
					if (posn < plan.size()) {
						int p = posn+1;
						if (p < plan.size()) {
							nestedLoop2(shard, plan, p, context, tree, writeLock, fromList, fromMap, selectColumnNames, resultsList, slaveResultsList, masterQuery);
						} else {
							Map<CollectionAlias, RecordId> masterMap = null;
							Map<CollectionAlias, String> slaveMap = null;
							
							if (masterQuery) {
								masterMap = new HashMap<CollectionAlias, RecordId>();
							} else {
								slaveMap = new HashMap<CollectionAlias, String>();
							}
							for (int i = 0; i < plan.size(); i++) {
								ca = plan.get(i).getCollectionAlias();
								
								RecordId recId = context.get(ca);
//								recId = resultContent.get();
								if (masterQuery) {
									masterMap.put(ca, recId);
								} else {
									StringType objectId = (StringType) context.getValue(ca, 0, null);
									slaveMap.put(ca, objectId.get());
									slaveResultsList.add(slaveMap);
								}
							}
							resultsList.add(masterMap);
						}
					} 
				} else if (plan.get(posn).continueOnMiss()){
					continue;
				} else {
					break;
				}
			}
			context.map.remove(plan.get(posn).getCollectionAlias());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			iter.unlock();			
		}
	}
	
	public static boolean filterTree(DataContext context, SimpleNode tree, Map<String, CollectionAlias> fromMap) {
		if (tree == null) {
			return true;
		}

		QueryEvaluator  qe = new QueryEvaluator(fromMap, context);
		return qe.processSelectStmt(tree);
	}	
}
