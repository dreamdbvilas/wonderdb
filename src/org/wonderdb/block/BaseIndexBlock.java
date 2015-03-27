package org.wonderdb.block;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;


public abstract class BaseIndexBlock extends BaseBlock 
	implements IndexBlock {

	BlockPtr parentPtr = null;
	protected DBType maxKey = null;
	
	public BaseIndexBlock(BlockPtr ptr) {
		super(ptr);
	}
	
	public int findPosn(IndexQuery entry, boolean writeLock, Set<Object> pinnedBlocks, Stack<BlockPtr> callBlockStack) {
		callBlockStack.push(this.getPtr());
		int mid = Collections.binarySearch(getData(), entry, entry.getComparator());
		if (mid < 0) {
			mid = Math.abs(mid) -1;
		}
		return mid;
	}
	
	public DBType getMaxKey(TypeMetadata meta) {
		readLock();
		try {
			if (maxKey == null) {
				List<Record> list = getData();
				if (list == null || list.size() == 0) {
					maxKey = null;
					return null;
				} else {
					Set<Object> pinnedBlocks = new HashSet<Object>();
					try {
						Record record = list.get(list.size()-1);
						DBType value = null;
						if (record instanceof ObjectRecord) {
							DBType column = ((ObjectRecord) record).getColumn();
							value = column;
							if (value instanceof BlockPtr) {
								IndexBlock block = (IndexBlock) BlockManager.getInstance().getBlock((BlockPtr) value, meta, pinnedBlocks);
								maxKey = block.getMaxKey(meta);
							} else if (value instanceof IndexKeyType) {
								maxKey = value;
							} else if (value instanceof ExtendedColumn) {
								maxKey = ((ExtendedColumn) value).getValue(meta);
							} else {
								throw new RuntimeException("unexpected type received");							
							}
						} else {
							throw new RuntimeException("unexpected type received");
						}
						maxKey = maxKey.copyOf();
					} finally {
						CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
					}
				}
			}
		} finally {
			readUnlock();
		}
		if (maxKey == null) {
			Logger.getLogger(getClass()).fatal("max key was null");
		}
		return maxKey;
	}
	
	public void addEntry(int posn, Record data) {
		getData().add(posn, data);
		if (posn+1 == getData().size()) {
			this.setMaxKey(((ObjectRecord) data).getColumn());			
		}
	}
		
	public void setMaxKey(DBType key) {
		if (key != null) {
			maxKey = key.copyOf();
		}
	}
	
	public BlockPtr getParent(Stack<BlockPtr> stack) {
		return stack.isEmpty() ? null : stack.pop();
	}
	
	public void setParent(BlockPtr ptr) {
		this.parentPtr = ptr != null ? (BlockPtr) ptr.copyOf() : null;
	}			
}
