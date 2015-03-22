package org.wonderdb.core.collection;

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

import java.util.Set;

import org.wonderdb.block.IndexQuery;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.IndexKeyType;


public interface BTree {
	void insert(IndexKeyType entry, Set<Object> pinnedBlocks, TransactionId txnId);
	public IndexKeyType remove(IndexKeyType entry, Set<Object> pinnedBlocks, TransactionId txnId);
	ResultIterator find(IndexQuery entry, boolean writeLock, Set<Object> pinnedBlocks);
	ResultIterator iterator();
	
	ResultIterator getHead(boolean writeLock, Set<Object> pinnedBlocks);
	
	public void readLock();
	public void readUnlock();
	public void writeLock();
	public void writeUnlock();
//	public String getSchemaObjectName();
}
