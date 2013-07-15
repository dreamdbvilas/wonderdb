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
package org.wonderdb.collection;

import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.impl.base.IndexQuery;
import org.wonderdb.cluster.Shard;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.impl.IndexKeyType;


public interface BTree {
	Shard getShard();
	void insert(IndexKeyType entry, Set<BlockPtr> pinnedBlocks, TransactionId txnId);
	public void remove(IndexKeyType entry, Set<BlockPtr> pinnedBlocks, TransactionId txnId);
	ResultIterator find(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks);
	ResultIterator getHead(boolean writeLock, Set<BlockPtr> pinnedBlocks);
	
	public void readLock();
	public void readUnlock();
	public void writeLock();
	public void writeUnlock();
	public int getSchemaId();
}
