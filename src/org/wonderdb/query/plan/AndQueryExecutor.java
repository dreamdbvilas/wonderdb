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
package org.wonderdb.query.plan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.collection.ValueNotAvailableException;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.StaticOperand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.StringType;



public class AndQueryExecutor {
	private static AndQueryExecutor instance = new AndQueryExecutor();
	private AndQueryExecutor() {
	}
	
	public static AndQueryExecutor getInstance() {
		return instance;
	}
	
	public void execute(Shard shard, List<QueryPlan> plan, List<BasicExpression> expList, boolean writeLock,
			List<CollectionAlias> fromList, Map<CollectionAlias, List<ColumnType>> selectColumnNames,
			List<Map<CollectionAlias, RecordId>> resultsList, List<Map<CollectionAlias, String>> slaveResultsList, boolean masterQuery) {
//		List<Map<CollectionAlias, RecordId>> results = new ArrayList<Map<CollectionAlias,RecordId>>();
		nestedLoop1(shard, plan, 0, null, expList, writeLock, fromList, selectColumnNames, resultsList, slaveResultsList, masterQuery);
	}

	private void nestedLoop1(Shard shard, List<QueryPlan> plan, int posn, DataContext context, 
			List<BasicExpression> expList, boolean writeLock,
			List<CollectionAlias> fromList, Map<CollectionAlias, List<ColumnType>> selectColumnNames,
			List<Map<CollectionAlias, RecordId>> resultsList, List<Map<CollectionAlias, String>> slaveResultsList, boolean masterQuery) {
	
		if (posn == 0) {
			context = new DataContext();
		}
		
		if (posn >= plan.size()) {
			return;
		}
		ResultIterator iter = null;
		RecordBlock lockedBlock = null;
		try {
			iter = plan.get(posn).iterator(context, shard, selectColumnNames.get(posn), writeLock);
			while (iter.hasNext()) {
				ResultContent resultContent = iter.next();
				context.add(plan.get(posn).getCollectionAlias(), resultContent);
				boolean filter;
				filter = filter(context, expList);
				if (filter) {
					if (resultContent instanceof IndexResultContent) {
						context.map.remove(plan.get(posn).getCollectionAlias());
						String collectionName = fromList.get(posn).getCollectionName();
						int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
						Set<BlockPtr> pinnedBlock = new HashSet<BlockPtr>();
						try {
							lockedBlock = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(resultContent.getRecordId().getPtr(), schemaId, pinnedBlock);
							lockedBlock.readLock();
							TableRecord tr = (TableRecord) CacheObjectMgr.getInstance().getRecord(lockedBlock, resultContent.getRecordId(), 
									selectColumnNames.get(posn), schemaId, pinnedBlock);
							StaticTableResultContent strc = new StaticTableResultContent(tr, resultContent.getRecordId(), schemaId);
							context.map.put(plan.get(posn).getCollectionAlias(), strc);
							filter = filter(context, expList);
							if (!filter) {
								continue;
							}
						} finally {
							lockedBlock.readUnlock();
							CacheEntryPinner.getInstance().unpin(pinnedBlock, pinnedBlock);
						}
					}
					if (posn < plan.size()) {
						int p = posn+1;
						if (p < plan.size()) {
							nestedLoop1(shard, plan, p, context, expList, writeLock, fromList, selectColumnNames, resultsList, slaveResultsList, masterQuery);
						} else {
							Map<CollectionAlias, RecordId> masterMap = null;
							Map<CollectionAlias, String> slaveMap = null;
							
							if (masterQuery) {
								masterMap = new HashMap<CollectionAlias, RecordId>();
							} else {
								slaveMap = new HashMap<CollectionAlias, String>();
							}
							for (int i = 0; i < plan.size(); i++) {
								CollectionAlias ca = plan.get(i).getCollectionAlias();
								
								RecordId recId = context.get(ca);
//								recId = resultContent.get();
								if (masterQuery) {
									masterMap.put(ca, recId);
									resultsList.add(masterMap);
								} else {
									StringType objectId = (StringType) context.getValue(ca, new ColumnType(0), null);
									slaveMap.put(ca, objectId.get());
									slaveResultsList.add(slaveMap);
								}
							}
						}
					} 
//					if (posn == plan.size()-1){
//						if (trMapList != null) {
//							Map<CollectionAlias, TableRecord> trMap = new HashMap<CollectionAlias, TableRecord>();
//							filter(context, fromList, selectColumnNames, trMap);
//							trMapList.add(trMap);
//						}
//					}
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
	
//	private List<DataContext> nestedLoop(List<QueryPlan> plan, List<BasicExpression> expList, boolean writeLock) {
//		DataContext dataContext = new DataContext();		
//		List<DataContext> result = new ArrayList<DataContext>();
//		List<ResultIterator> iters = new ArrayList<ResultIterator>(plan.size());
//		
//		// lets lock trees in proper order
//		Set<String> set = new HashSet<String>();
//		for (int i = 0; i < plan.size(); i++) {
//			QueryPlan p = plan.get(i);
//			if (p instanceof IndexRangeScan) {
//				set.add(((IndexRangeScan) p).getIndex().getIndexName());
//			}
//		}
//		
//		Iterator<String> it = set.iterator();
//		while (it.hasNext()) {
//			SchemaMetadata.getInstance().getIndex(it.next()).getIndexTree().readLock();
//		}
//		int posn = 0;		
//		iters.add(plan.get(0).iterator(dataContext, writeLock));
//		
//		while (true) {
//			if (iters.get(posn).hasNext()) {
//				// first clear all 
//				if (posn == 0) {
//					dataContext = new DataContext();
//				}
//				RecordId recordId = iters.get(posn).next();
//				
//				dataContext.add(plan.get(posn).getCollectionAlias(), recordId);
//				boolean done = false;
//				if (dataContext.map.size() == plan.size() ) {
//					if (filter(dataContext, expList)) {
//						DataContext tmp =  new DataContext();
//						tmp.map = new HashMap<CollectionAlias, RecordId>(dataContext.map);;
//						result.add(tmp);
//						done = false;
//					} else {
//						if (!plan.get(posn).continueOnMiss()) {
//							done = true;
//						}
//					}
//				}
//				if (done) {
//					if (posn == 0) {
//						break;
//					}
//					
//					if ((posn+1)%plan.size() == 0) {
//						posn--;
//					} else {
//						posn++;
//					}					
//				} else {
//					if (((posn+1) % plan.size() != 0)) {
//						iters.get(posn).unlock();
//						posn++;
//						if (iters.size()-1 < posn) {
//							iters.add(plan.get(posn).iterator(dataContext, writeLock));
//						} else {
//							iters.set(posn, plan.get(posn).iterator(dataContext, writeLock));
//						}
//					}
//				}
//			} else {
//				if (posn == 0) {
//					break;
//				}
//				// we have reached the end. lets go up to higher iterator.
//				posn--;
//			}
//		}
//		
//		for (int i = 0; i < iters.size(); i++) {
//			ResultIterator iter = iters.get(i);
//			iter.unlock();
//		}
//		it = set.iterator();
//		return result;		
//	}
	
	public static boolean filter(DataContext context, List<BasicExpression> expList) {
		if (expList == null) {
			return true;
		}
		for (int i = 0; i < expList.size(); i++) {
			BasicExpression exp = expList.get(i);
			Operand right = exp.getRightOperand();
			Operand left = exp.getLeftOperand();
			DBType rVal = null;
			DBType lVal = null;
			boolean contextAvailable = true;
			
			if (right instanceof VariableOperand) {
				VariableOperand r = (VariableOperand) right;
				try {
					rVal = context.getValue(r.getCollectionAlias(), r.getColumnType(), r.getPath());
				} catch (ValueNotAvailableException e) {
					contextAvailable = false;
				}
				if (context.get(r.getCollectionAlias()) == null) {
					contextAvailable = false;
				}
			} else {
				StaticOperand r = (StaticOperand) right;
				rVal = (DBType) r.getValue(null);
			}

			if (left instanceof VariableOperand) {
				VariableOperand l = (VariableOperand) left;
				try {
					lVal = context.getValue(l.getCollectionAlias(), l.getColumnType(), l.getPath());
				} catch (ValueNotAvailableException e) {
					contextAvailable = false;
				}
				if (context.get(l.getCollectionAlias()) == null) {
					contextAvailable = false;
				}
			} else {
				StaticOperand l = (StaticOperand) left;
				lVal = (DBType) l.getValue(null);
			}
			
//			if (rVal == null || lVal == null || ExpressionEvaluator.getInstance().evaluate(lVal, rVal, exp) == 0) {
			if (!contextAvailable || ExpressionEvaluator.getInstance().evaluate(lVal, rVal, exp) == 0) {
				continue;
			} else {
				return false;
			}
		}
		
		return true;
	}	
}
