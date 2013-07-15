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
package org.wonderdb.block.index.impl.base;

import java.util.List;
import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.IndexData;
import org.wonderdb.block.index.IndexLeafBlock;
import org.wonderdb.collection.impl.BlockEntryPosition;
import org.wonderdb.types.DBType;


public abstract class BaseIndexLeafBlock extends BaseIndexBlock 
	implements IndexLeafBlock, IndexData {
	
	boolean splitRequired = false;
	List<BlockPtr> contPtrList = null;
	
	public BaseIndexLeafBlock(BlockPtr  ptr, int schemaObjectId) {
		super(ptr, schemaObjectId);
//		size = getByteSize();
	}
			
	public BlockEntryPosition find(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks) {
		if (writeLock) {
			writeLock();
		} else {
			readLock();
		}
		return super.find(entry, writeLock, pinnedBlocks);
	}

//	public int findPosn(IndexQuery entry, boolean isUnique, boolean writeLock) {
//		return super.findPosn(entry, writeLock);		
//	}

	public boolean isSplitRequired() {
		return splitRequired;
	}
	
	public void setSplitRequired(boolean flag) {
		splitRequired = flag;
	}

	@Override
	public List<BlockPtr> getContPtrList() {
		return contPtrList; 
	}
	
	@Override
	public void setContPtrList(List<BlockPtr> ptrList) {
		contPtrList = ptrList;
	}
	
	public DBType getKey() {
		return getMaxKey();
	}
	
	public DBType getMaxKey() {
		DBType retVal = null;
		readLock();
		retVal = maxKeyRef.get();
		try {
			if (maxKeyRef.get() == null) {
				DBType dt = (DBType) getData().get(getData().size()-1);
				maxKeyRef.set(dt);
				retVal = dt;
			}
		} finally {
			readUnlock();
		}
		return retVal;
	}
}
