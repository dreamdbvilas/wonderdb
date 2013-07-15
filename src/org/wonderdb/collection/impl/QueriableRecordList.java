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
package org.wonderdb.collection.impl;

import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.cluster.Shard;

public interface QueriableRecordList extends BlockRecordList<QueriableBlockRecord> {
	public Shard getShard();
//
//	public abstract RecordBlock getHead(boolean writeLock,
//			Set<BlockPtr> pinnedBlocks);
//
//	public abstract RecordBlock add(QueriableBlockRecord record);
//
//	public abstract void update(RecordId recordId, Map<ColumnType, DBType> changedColumns);
//
//	public abstract void delete(RecordId recordId);

}