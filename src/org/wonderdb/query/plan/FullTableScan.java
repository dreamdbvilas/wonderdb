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

import java.util.List;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockEntryPosition;
import org.wonderdb.block.BlockManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.TableResultContent;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.core.collection.impl.BaseResultIteratorImpl;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.Record;
import org.wonderdb.types.record.TableRecord;



public class FullTableScan implements QueryPlan {
	CollectionAlias collectionAlias;
	ResultIterator iter = null;
	Set<Object> pinnedBlocks = null;
	TypeMetadata meta = null;
	WonderDBList recordList = null;
	
	public FullTableScan(WonderDBList recordList, CollectionAlias ca, Set<Object> pinnedBlocks) {
		collectionAlias = ca;
		this.pinnedBlocks = pinnedBlocks;
		this.recordList = recordList;
		meta = SchemaMetadata.getInstance().getTypeMetadata(ca.getCollectionName());
	}
	
	public void setDependentCollections(Set<CollectionAlias> ca) {
	}
	
	public CollectionAlias getCollectionAlias() {
		return collectionAlias;
	}

	public ResultIterator iterator(DataContext context, Shard shard, List<Integer> selectColumnList, boolean writeLock) {
		TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(collectionAlias.getCollectionName());
 		BlockPtr head = recordList.getRealHead(pinnedBlocks, meta);
 		Block block = BlockManager.getInstance().getBlock(head, meta, pinnedBlocks);
 		block.readLock();
		ResultIterator iter = new FullTableScanIterator(new BlockEntryPosition(block, 0));
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
		String schemaObjectName = null;
		
		private FullTableScanIterator(BlockEntryPosition bep) {
			super(bep, false, meta);
		}
		
		public Block getBlock(BlockPtr ptr, TypeMetadata meta) {
			return BlockManager.getInstance().getBlock(ptr, meta, pinnedBlocks);
		}
		
		public void insert(Record c) {
			throw new RuntimeException("Method not supported");
		}
		
		public Block getCurrentRecordBlock() {
			return iter.getCurrentBlock();
		}
		
		public Record next() {
			TableRecord record = (TableRecord) super.next();
			return new TableResultContent(record, meta);
		}

//		@Override
//		public String getSchemaObjectName() {
//			return schemaObjectName;
//		}
	}
}
