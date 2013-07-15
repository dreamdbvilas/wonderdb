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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.impl.base.BaseBlock;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.IndexData;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.collection.impl.BlockEntryPosition;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.IndexKeyType;


public abstract class BaseIndexBlock extends BaseBlock 
	implements IndexBlock, IndexData {
	BlockPtr parentPtr = null;
	protected AtomicReference<DBType> maxKeyRef = new AtomicReference<DBType>(null);
	
	public BaseIndexBlock(BlockPtr ptr, int schemaObjectId) {
		super(ptr, schemaObjectId);
	}
	
	public DBType getKey() {
		return getMaxKey();
	}
	
	public int findPosn(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks) {
		
		int start = 0;
		int end = getData().size()-1;
		CacheableList list = getData();
		int mid=list.size();
		int c = 0;
		if (getData().size() > 0) {
			while (start <= end) {
				mid = (start+end)/2;
				IndexData data = (IndexData) list.getAndPin(mid, pinnedBlocks);
				DBType key = data.getKey();
				c = entry.compareTo((IndexKeyType) key);
				
				if (c <= 0) {
					end = mid-1;
				} else {
					start = mid+1;
				} 
			}
		}
		mid = Math.max(start, end);
		return mid;
	}	

	public BlockEntryPosition find(IndexQuery entry, boolean writeLock, Set<BlockPtr> pinnedBlocks) {
		int mid = findPosn(entry, writeLock, pinnedBlocks);
		CacheableList list = getData();
		IndexData data = null;
		
		if (list.size() > mid) {
			data = (IndexData) list.getAndPin(mid, pinnedBlocks);
		}
		
		if (data instanceof BaseIndexBlock) {
			BaseIndexBlock b = (BaseIndexBlock) data;		
			return b.find(entry, writeLock, pinnedBlocks);
		}
		
		return new BlockEntryPosition(this, mid);
	}
	
	public DBType getMaxKey() {
		DBType maxKey = maxKeyRef.get();
		if (maxKey == null) {
			CacheableList list = getData();
			if (list == null || list.size() == 0) {
				maxKey = null;
				return null;
			} else {
				Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
				try {
					IndexData data = (IndexData) list.getAndPin(list.size()-1, pinnedBlocks);
					if (data != null) {
						DBType dt = data.getKey();
						maxKey = dt.copyOf();
						maxKeyRef.set(maxKey);
					}
				} finally {
					CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
				}
			}
		}

		if (maxKey == null) {
			if (maxKeyRef.get() == null) {
				Logger.getLogger(getClass()).fatal("max key was null");
			}
		}
		return maxKey;
	}
	
	public int addEntry(int posn, IndexData data) {
		int addPosn = super.addEntry(posn, data);
		if (addPosn+1 == getData().size()) {
			this.setMaxKey(data.getKey());			
		}
		return addPosn;
	}
	
	
	public void setMaxKey(DBType key) {
		if (key != null) {
			DBType maxKey = key.copyOf();
			maxKeyRef.set(maxKey);
		}
	}
	
	public BlockPtr getParent() {
		return parentPtr;
	}
	
	public void setParent(BlockPtr ptr) {
		this.parentPtr = ptr;
	}	
	
	@Override
	public int getByteSize() {
		int size = super.getByteSize();
		return size + BlockPtrSerializer.BASE_SIZE;
	}
	
	@Override
	public int evict(long ts) {
		return 0;
	}
	
	@Override
	public boolean canFullEvict(long ts) {
		return true;
	}
	
	@Override
	public void setBufferCount(int count) {
		bufferCount = count;
	}
//
//	@Override
//	public int getBlockCount() {
//		return blockCount;
//	}
}
