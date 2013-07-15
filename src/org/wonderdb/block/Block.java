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
package org.wonderdb.block;

import org.wonderdb.types.Cacheable;




public interface Block extends ExternalReference<BlockPtr, CacheableList>, Cacheable {
	BlockPtr getNext();
	BlockPtr getPrev();
	BlockPtr getParent();
	
	void setNext(BlockPtr n);
	void setPrev(BlockPtr p);
	void setParent(BlockPtr p);
	
	void writeLock();
	void writeUnlock();
	void readLock();
	void readUnlock();
	
	public int getSchemaObjectId();
	public int addEntry(int posn, Cacheable c);
	public int getBlockCount();
}
