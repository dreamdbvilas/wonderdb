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

import java.util.List;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.collection.TableResultContent;
import org.wonderdb.collection.impl.BaseResultIteratorImpl;
import org.wonderdb.collection.impl.BlockEntryPosition;
import org.wonderdb.collection.impl.QueriableRecordList;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.impl.ColumnType;



public class FullTableScan implements QueryPlan {
	CollectionAlias collectionAlias;
	ResultIterator iter = null;
	Set<BlockPtr> pinnedBlocks = null;
	
	public FullTableScan(CollectionAlias ca, Set<BlockPtr> pinnedBlocks) {
		collectionAlias = ca;
		this.pinnedBlocks = pinnedBlocks;
	}
	
	public void setDependentCollections(Set<CollectionAlias> ca) {
	}
	
	public CollectionAlias getCollectionAlias() {
		return collectionAlias;
	}

	public ResultIterator iterator(DataContext context, Shard shard, List<ColumnType> selectColumnList, boolean writeLock) {
		CollectionMetadata collectionMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionAlias.collectionName);
		if (collectionMeta == null) {
			return null;
		}
		QueriableRecordList recordList = collectionMeta.getRecordList(shard);
		RecordBlock head = recordList.getHead(writeLock, selectColumnList, pinnedBlocks);
		ResultIterator iter = new FullTableScanIterator(new BlockEntryPosition(head, 0));
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
		return iter.getCurrentBlock();
	}
	
	public boolean continueOnMiss() {
		return true;
	}
	
	public class FullTableScanIterator extends BaseResultIteratorImpl {
		private FullTableScanIterator(BlockEntryPosition bep) {
			super(bep, false);
		}
		
//		public void lock(Block block) {
//			block.readLock();
//		}
		
		public Block getBlock(BlockPtr ptr) {
			return CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(ptr, currentPosn.getBlock().getSchemaObjectId(), pinnedBlocks);
		}
		
		public int insert(Cacheable c) {
			throw new RuntimeException("Method not supported");
		}
		
		public Block getCurrentRecordBlock() {
			return iter.getCurrentBlock();
		}
		
		public ResultContent next() {
			TableRecord record = (TableRecord) super.nextEntry();
			int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionAlias.collectionName).getSchemaId();
			return new TableResultContent((RecordBlock) super.getCurrentBlock(), record.getRecordId(), schemaId);
		}
	}
}
