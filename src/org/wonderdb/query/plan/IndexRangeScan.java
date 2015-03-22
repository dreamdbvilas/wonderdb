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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexCompareIndexQuery;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.impl.BTreeIteratorImpl;
import org.wonderdb.core.collection.impl.BaseResultIteratorImpl;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Expression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.StaticOperand;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.Record;
import org.wonderdb.types.record.TableRecord;




public class IndexRangeScan implements QueryPlan {
	IndexNameMeta idx;
	CollectionAlias collectionAlias;
	List<BasicExpression> expList = null;
	Map<CollectionAlias, Integer> executionOrder = new HashMap<CollectionAlias, Integer>();
	BTree tree = null;
	boolean continueOnMiss = false;
	Set<CollectionAlias> dependentCollections = null;
	IndexRangeScanIterator iter = null;
	Set<Object> pinnedBlocks = null;
	TypeMetadata meta = null;
	
	public CollectionAlias getCollectionAlias() {
		return collectionAlias;
	}
	
	public void setDependentCollections(Set<CollectionAlias> s) {
		dependentCollections = s;
	}
	
	public IndexRangeScan(CollectionAlias ca, IndexNameMeta idx, Shard shard, List<BasicExpression> list, List<CollectionAlias> executionOrder, Set<Object> pinnedBlocks) {
		this.idx = idx;
		collectionAlias = ca;
		expList = list;
		tree = SchemaMetadata.getInstance().getIndex(idx.getIndexName()).getIndexTree(shard);
		this.pinnedBlocks = pinnedBlocks;
		meta = SchemaMetadata.getInstance().getIndexMetadata(idx);
	}
	
	public IndexNameMeta getIndex() {
		return idx;
	}

	public 	Iterator<TableRecord> recordIterator(DataContext dataContext) {
		throw new RuntimeException("Method not supported");
	}
	
	public ResultIterator iterator(DataContext dataContext, Shard shard, List<Integer> selectColumns, boolean writeLock) {
		Shard indexShard = new Shard(idx.getIndexName());
		BTree tree = idx.getIndexTree(indexShard);
		IndexRangeScanIterator iter = null;
		if (expList.size() != 0) {
			iter = new IndexRangeScanIterator(idx, indexShard, collectionAlias, expList, dataContext, writeLock, pinnedBlocks);
		} else {
			continueOnMiss = true;
			iter = new IndexRangeScanIterator(tree.getHead(writeLock, pinnedBlocks), shard, selectColumns, pinnedBlocks, idx.getIndexName());
		}
		this.iter = iter;
		return iter;
	}	
	
	public Block getCurrentBlock() {
		if (iter == null) {
			return null;
		}
		return iter.getCurrentBlock();
	}
	
	public Block getCurrentRecordBlock() {
		if (iter == null) {
			return null;
		}
		return iter.getCurrentRecordBlock();
	}
	
	public boolean continueOnMiss() {
		return continueOnMiss;
	}
	
	public class IndexRangeScanIterator implements ResultIterator {
		BaseResultIteratorImpl iter;
		DataContext context = null;
		boolean hasNext = true;
		IndexNameMeta idx;
		CollectionAlias collectionAlias = null;
		List<Integer> selectColumns = null;
		Shard shard = null;
		String schemaObjectName = null;
		IndexKeyType min = null;
		IndexKeyType max = null;
		boolean includeMin = false;
		boolean includeMax = false;
		
		Record currentRecord = null;
		
		public IndexRangeScanIterator(ResultIterator iter, Shard shard, List<Integer> selectColumns, Set<Object> pinnedBlocks, String schemaObjectName) {
			this.iter = (BaseResultIteratorImpl) iter;
			this.selectColumns = selectColumns;
			this.shard = shard;
			this.schemaObjectName = schemaObjectName;
		}
		
		public IndexRangeScanIterator(IndexNameMeta idx, Shard shard, CollectionAlias ca, List<BasicExpression> expList, 
				DataContext context, boolean writeLock, Set<Object> pinnedBlocks) {
			this.context = context;
			this.idx = idx;
			this.collectionAlias = ca;

			List<DBType> list = new ArrayList<>(idx.getColumnIdList().size());
			for (int i = 0; i < idx.getColumnIdList().size(); i++) {
				list.add(null);
			}
			min = new IndexKeyType(list, null);
			list = new ArrayList<>(list);
			max = new IndexKeyType(list, null);
			buildRange();

			iter = (BTreeIteratorImpl) getIterator(shard, expList, context, writeLock);
		}
		
		private void buildRange() {
			for (int i = 0; i < expList.size(); i++) {
				BasicExpression exp = expList.get(i);
				Operand left = exp.getLeftOperand();
				Operand right = exp.getRightOperand();
				DBType value = null;
				
				if (left instanceof VariableOperand) {
					VariableOperand vo = (VariableOperand) left;
					if (((VariableOperand) left).getCollectionAlias().equals(collectionAlias)) {
						if (idx.getColumnIdList().contains(vo.getColumnId())) {
							if (right instanceof StaticOperand) {
								value = right.getValue((TableRecord) null, null);
							} else if (right instanceof VariableOperand){
								if (idx.getColumnIdList().contains(((VariableOperand) right).getColumnId())) {
									value = context.getValue(((VariableOperand) right).getCollectionAlias(), ((VariableOperand) right).getColumnId(), null);
								}
							}
							updateMinMax((VariableOperand) left, value, exp.getOperator());
						}
					}
				} else if (left instanceof StaticOperand) {
					value = left.getValue((TableRecord) null, null);
					if (right instanceof VariableOperand) {
						VariableOperand vo = (VariableOperand) right;
						if (vo.getCollectionAlias().equals(collectionAlias)) {
							if (idx.getColumnIdList().contains(vo.getColumnId())) {
								updateMinMax((VariableOperand) right, value, invertOp(exp.getOperator()));
							}
						}
					}
				}
			}
		}
		
		private int invertOp(int op) {
			switch(op) {
			case Expression.EQ:
			case Expression.LIKE:
				return op;
			case Expression.LE:
				return Expression.GE;
			case Expression.LT:
				return Expression.GT;
			case Expression.GE:
				return Expression.LE;
			case Expression.GT:
				return Expression.LT;
			}
			return op;
		}
		
		private void updateMinMax(VariableOperand left, DBType value, int op) {
			int posn = getPosn(left.getColumnId());
			switch (op) {
			case Expression.EQ:
				includeMin = true;
				includeMax = true;
				DBType dt1 = min.getValue().get(posn);
				if (dt1 == null || dt1.compareTo(value) > 0) {
					min.getValue().set(posn, value);
				}
				dt1 = max.getValue().get(posn);
				if (dt1 == null || dt1.compareTo(value) < 0) {
					max.getValue().set(posn, value);
				}
			break;
			
			case Expression.GE:
				includeMin = true;
			case Expression.GT:
				dt1 = min.getValue().get(posn);
				if (dt1 == null || dt1.compareTo(value) > 0) {
					min.getValue().set(posn, value);
				}
			break;
			
			case Expression.LE:
				includeMax = true;
			case Expression.LT:
				dt1 = max.getValue().get(posn);
				if (dt1 == null || dt1.compareTo(value) < 0) {
					max.getValue().set(posn, value);
				}
			break;
			}
		}
		
		private int getPosn(int colId) {
			for (int i = 0; i < idx.getColumnIdList().size(); i++) {
				if (colId == idx.getColumnIdList().get(i)) {
					return i;
				}
			}
			return -1;
		}
		
		@Override
		public void insert (Record c) {
			throw new RuntimeException("Method not supported");
		}
		
		@Override
		public Record next() {
			IndexRecord key = (IndexRecord) currentRecord;
			if (key == null) {
				return null;
			}
			DBType column = key.getColumn();
			if (column == null) {
				return null;
			}
			if (column instanceof ExtendedColumn) {
				column = (IndexKeyType) ((ExtendedColumn) column).getValue(meta);
			}
			return new IndexResultContent((IndexKeyType) column, meta, pinnedBlocks);
		}
				
		public Block getCurrentBlock() {
			return iter.getCurrentBlock();
		}
		
		public Block getCurrentRecordBlock() {
			IndexRecord record = (IndexRecord) iter.next();
			IndexKeyType key = (IndexKeyType) record.getColumn();
			if (key == null) {
				return null;
			}
//			BlockEntryPosition bep = key.getBlockEntryPosn();
			RecordId recordId = key.getRecordId();
			CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
			Block b = (Block) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, pinnedBlocks); 
			return b;
		}
		
		public boolean hasNext() {
			boolean val = iter.hasNext();
			if (!val) {
				return val;
			}
			
			IndexRecord record = (IndexRecord) iter.next();
			currentRecord = record;
			DBType column = record.getColumn();
			if (column instanceof ExtendedColumn) {
				column = (IndexKeyType) ((ExtendedColumn) column).getValue(meta);
			}
			IndexKeyType ikt = (IndexKeyType) (column instanceof ExtendedColumn ? ((ExtendedColumn) column).getValue(meta) : column);
			for (int i= 0; i < min.getValue().size(); i++) {
				DBType minValue = min.getValue().get(i);
				DBType maxValue = max.getValue().get(i);
				DBType value = ikt.getValue().get(i);
				
				if ((minValue == null || value.compareTo(minValue) >= 0) &&
				   ((maxValue == null || (value.compareTo(maxValue) >= 0 && includeMax)) || (value.compareTo(maxValue) < 0))) {					
					continue;
				} else {
					return false;
				}
			}
			return true;
		}
		
		public boolean isAnyBlockEmpty() {
			throw new RuntimeException("Method not supported");
		}
		
		public void remove() {
			iter.remove();
		}
		
		public void lock(Block b) {
			iter.lock(b);
		}
		
		public void unlock() {
			iter.unlock();
		}
		
		private  ResultIterator getIterator(Shard shard, List<BasicExpression> expList, DataContext context, boolean writeLock) {
			Shard indexShard = new Shard(idx.getIndexName());
			BTree tree = SchemaMetadata.getInstance().getIndex(idx.getIndexName()).getIndexTree(indexShard);
//			ExpressionFilterIndexQuery query = new ExpressionFilterIndexQuery(expList, context, idx, collectionAlias);
			TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(idx);
			IndexCompareIndexQuery query = new IndexCompareIndexQuery(min, includeMin, meta, pinnedBlocks);
			return tree.find(query, writeLock, pinnedBlocks);
		}
		@Override
		public void unlock(boolean shouldUnpin) {
			unlock();
		}

		@Override
		public Set<Object> getPinnedSet() {
			return iter.getPinnedSet();
		}

		@Override
		public TypeMetadata getTypeMetadata() {
			return iter.getTypeMetadata();
		}

//		@Override
//		public String getSchemaObjectName() {
//			return schemaObjectName;
//		}
	}
}
