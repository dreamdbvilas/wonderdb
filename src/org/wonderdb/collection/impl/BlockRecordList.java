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

import java.util.List;
import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.impl.ColumnType;


public interface BlockRecordList<Data extends QueriableBlockRecord> {

	public abstract RecordBlock getHead(boolean writeLock, List<ColumnType> selectColumns, Set<BlockPtr> pinnedBlocks);

	public abstract RecordBlock add(Data record, Set<BlockPtr> pinnedBlocks, TransactionId txnId);

	public abstract void update(RecordId recordId, Data changedColumns, Set<BlockPtr> pinnedBlocks, TransactionId txnId);

	public abstract void delete(RecordId recordId, Set<BlockPtr> pinnedBlocks, TransactionId txnId);


}
