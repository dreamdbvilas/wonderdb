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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.index.impl.base.ExpressionFilterIndexQuery;
import org.wonderdb.block.index.impl.base.Queriable;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.PrimaryCacheHandlerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.BTree;
import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.collection.impl.BTreeIteratorImpl;
import org.wonderdb.collection.impl.BaseResultIteratorImpl;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;



public class IndexRangeScan implements QueryPlan {
	Index idx;
	CollectionAlias collectionAlias;
	List<BasicExpression> expList = null;
	Map<CollectionAlias, Integer> executionOrder = new HashMap<CollectionAlias, Integer>();
	BTree tree = null;
	boolean continueOnMiss = false;
	Set<CollectionAlias> dependentCollections = null;
	IndexRangeScanIterator iter = null;
	Set<BlockPtr> pinnedBlocks = null;
	
	public CollectionAlias getCollectionAlias() {
		return collectionAlias;
	}
	
	public void setDependentCollections(Set<CollectionAlias> s) {
		dependentCollections = s;
	}
	
	public IndexRangeScan(CollectionAlias ca, Index idx, Shard shard, List<BasicExpression> list, List<CollectionAlias> executionOrder, Set<BlockPtr> pinnedBlocks) {
		this.idx = idx;
		collectionAlias = ca;
		expList = list;
		tree = SchemaMetadata.getInstance().getIndex(idx.getIndexName()).getIndexTree(shard);
		this.pinnedBlocks = pinnedBlocks;
	}
	
	public Index getIndex() {
		return idx;
	}

	public 	Iterator<TableRecord> recordIterator(DataContext dataContext) {
		throw new RuntimeException("Method not supported");
	}
	
	public ResultIterator iterator(DataContext dataContext, Shard shard, List<ColumnType> selectColumns, boolean writeLock) {
		IndexMetadata meta = SchemaMetadata.getInstance().getIndex(idx.getIndexName());
		Shard indexShard = new Shard(meta.getSchemaId(), meta.getName(), shard.getReplicaSetName());
		BTree tree = meta.getIndexTree(indexShard);
		IndexRangeScanIterator iter = null;
		if (expList.size() != 0) {
			iter = new IndexRangeScanIterator(idx, indexShard, collectionAlias, expList, dataContext, writeLock, pinnedBlocks);
		} else {
			continueOnMiss = true;
			iter = new IndexRangeScanIterator(tree.getHead(writeLock, pinnedBlocks), shard, selectColumns, pinnedBlocks);
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
	
	private static int binarySearch(CacheableList list, QueriableBlockRecord tr) {
		int retVal = 0;	
		boolean found = false;
		
		for (retVal = 0; retVal < list.size(); retVal++) {
			QueriableBlockRecord e = (QueriableBlockRecord) list.get(retVal);
			if (tr.getRecordId().getPosn() == e.getRecordId().getPosn()) {
				found = true;
				break;
			}
		}
		if (found) { 
			return retVal;
		} 
		return -1;
	}
	
	public class IndexRangeScanIterator implements ResultIterator {
		BaseResultIteratorImpl iter;
		DataContext context = null;
		boolean hasNext = true;
		Index idx;
		CollectionAlias collectionAlias = null;
		IndexMetadata idxMeta;
//		Set<BlockPtr> pinnedBlocks = null;
		List<ColumnType> selectColumns = null;
		Shard shard = null;
		
		public IndexRangeScanIterator(ResultIterator iter, Shard shard, List<ColumnType> selectColumns, Set<BlockPtr> pinnedBlocks) {
			this.iter = (BaseResultIteratorImpl) iter;
//			this.pinnedBlocks = pinnedBlocks;
			this.selectColumns = selectColumns;
			this.shard = shard;
		}
		
		public int insert (Cacheable c) {
			throw new RuntimeException("Method not supported");
		}
		
		public ResultContent next() {
			IndexKeyType key = (IndexKeyType) iter.nextEntry();
			if (key == null) {
				return null;
			}
//			BlockEntryPosition bep = key.getBlockEntryPosn();
			return new IndexResultContent(key, idxMeta.getSchemaId());
		}
		
//		public Queriable getCurrent() {
//			IndexKeyType key = (IndexKeyType) iter.nextEntry();
//			if (key == null) {
//				return null;
//			}
//			RecordId recordId = key.getRecordId();
//			
//			RecordBlock b = CacheObjectMgr.getInstance().loadRecordAndGetBlock(key.getRecordId(), selectColumns, idxMeta.getSchemaId(), pinnedBlocks); 
//			TableRecord rec = new TableRecord(recordId, null);
//			int posn = binarySearch(b.getData(), rec);
//			if (posn < 0) {
//				return null;
//			}
//			return (Queriable) b.getData().get(posn);
//		}
		
		
		public Block getCurrentBlock() {
			return iter.getCurrentBlock();
		}
		
		public Block getCurrentRecordBlock() {
			IndexKeyType key = (IndexKeyType) iter.nextEntry();
			if (key == null) {
				return null;
			}
//			BlockEntryPosition bep = key.getBlockEntryPosn();
			RecordId recordId = key.getRecordId();
			CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
			RecordBlock b = (RecordBlock) PrimaryCacheHandlerFactory.getInstance().getCacheHandler().get(recordId.getPtr()); 
			return b;
		}
		
		public IndexRangeScanIterator(Index idx, Shard shard, CollectionAlias ca, List<BasicExpression> expList, 
				DataContext context, boolean writeLock, Set<BlockPtr> pinnedBlocks) {
			this.context = context;
			this.idx = idx;
			idxMeta = SchemaMetadata.getInstance().getIndex(idx.getIndexName());
			this.collectionAlias = ca;
			iter = (BTreeIteratorImpl) getIterator(shard, expList, context, writeLock);
//			this.pinnedBlocks = pinnedBlocks;
		}
		
		public boolean hasNext() {
			return iter.hasNext();
		}
		
		public Queriable nextEntry() {
			IndexKeyType key = (IndexKeyType) iter.nextEntry();
			if (key == null) {
				return null;
			}
//			BlockEntryPosition bep = key.getBlockEntryPosn();
			RecordId recordId = key.getRecordId();
			CacheEntryPinner.getInstance().pin(recordId.getPtr(), pinnedBlocks);
			RecordBlock b = (RecordBlock) PrimaryCacheHandlerFactory.getInstance().getCacheHandler().get(recordId.getPtr());
			TableRecord rec = new TableRecord(recordId, null);
			int posn = binarySearch(b.getData(), rec);
			if (posn < 0) {
				return null;
			}
			return (Queriable) b.getData().get(posn);
		}
		
//		public BlockEntryPosition<Queriable> peek() {
//			BlockEntryPosition<IndexData> p = iter.peek();
//		}
		
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
			Shard indexShard = new Shard(idxMeta.getSchemaId(), idxMeta.getName(), shard.getReplicaSetName());
			BTree tree = SchemaMetadata.getInstance().getIndex(idx.getIndexName()).getIndexTree(indexShard);
			ExpressionFilterIndexQuery query = new ExpressionFilterIndexQuery(expList, context, idx, collectionAlias);
			return tree.find(query, writeLock, pinnedBlocks);
		}
		
		public Index getIndex() {
			return idx;
		}

		@Override
		public void unlock(boolean shouldUnpin) {
			unlock();
		}

		@Override
		public Set<BlockPtr> getPinnedSet() {
			return iter.getPinnedSet();
		}
		
		// comparison should be only for equals
		// > should be reduced to =
		// < should be ignored since < will be evaluated during walking the linklist. Assuming link list is in order which it is.
	}
}
