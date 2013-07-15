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
package org.wonderdb.block.impl.base;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.SerializableType;


public class BaseCacheableList extends ArrayList<Cacheable> implements CacheableList {
	private static final long serialVersionUID = -1030370438314951016L;
//	List<Cacheable> list = new ArrayList<Cacheable>();
	BaseBlock parentBlock = null;
	
//	long footprint = 0;
	int schemaObjId;
	
	public BaseCacheableList(BaseBlock parent, int schemaObjId) {
		this.schemaObjId = schemaObjId;
		parentBlock = parent;
	}
	
//	public int size() {
//		return list.size();
//	}
	
	@Override
	public boolean add(Cacheable c) {
//		footprint = footprint + c.getMemoryFootprint();
		if (c instanceof Block) {
			super.add(((Block) c).getPtr());
		} else {
			super.add(c);
		}
		
//		if (c instanceof SerializableType) {
//			parentBlock.addToSize(((SerializableType) c).getByteSize());
//		}
		return true;
	}

	@SuppressWarnings("unused")
	public Cacheable set(int i, Cacheable c) {
//		footprint = footprint + c.getMemoryFootprint();
		long origSize = 0;
		if (get(i) instanceof SerializableType) {
			origSize = ((SerializableType) get(i)).getByteSize();
		}
		Cacheable retVal = null;
		if (c instanceof Block) {
			retVal = super.set(i, ((Block) c).getPtr());
		} else {
			retVal = super.set(i, c);
		}

//		if (c instanceof SerializableType) {
//			parentBlock.removeFromSize(origSize);
//			parentBlock.addToSize(((SerializableType) c).getByteSize());
//		}	
		
		return retVal;
	}

	public void addAll(int posn, List<? extends Cacheable> c) {
//		footprint = footprint + c.getMemoryFootprint();
		Iterator<? extends Cacheable> iter = c.iterator();
		while (iter.hasNext()) {
			Cacheable v = iter.next();
			add(posn++, v);
		}
	}

	public void addAll(List<? extends Cacheable> c) {
//		footprint = footprint + c.getMemoryFootprint();
		Iterator<? extends Cacheable> iter = c.iterator();
		while (iter.hasNext()) {
			Cacheable v = iter.next();
			add(v);
		}
	}

	public void add(int i, Cacheable c) {
//		footprint = footprint + c.getMemoryFootprint();
		if (c instanceof Block) {
			super.add(i, ((Block) c).getPtr());
		} else {
			super.add(i, c);
		}

//		if (c instanceof SerializableType) {
//			parentBlock.addToSize(((SerializableType) c).getByteSize());
//		}
	}
	
	public Cacheable remove(int i) {
		Cacheable c = super.remove(i);
		return c;
	}
	
//	public long getMemoryFootprint() {
//		return footprint + (size() * Constants.POINTER_SIZE);
//	}
//	
	public Cacheable getUncached(int posn) {
		return super.get(posn);
	}
	
	@Override
	public Cacheable get(int posn) {
		Cacheable c = super.get(posn);
		if (c instanceof BlockPtr) {
			BlockPtr p = (BlockPtr) c;
			if (parentBlock instanceof IndexBlock) {
				return (Cacheable) CacheObjectMgr.getInstance().getIndexBlock(p, parentBlock.getSchemaObjectId(), null);
			} else {
				return (Cacheable) CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(p, parentBlock.getSchemaObjectId(), null);
			}
		}
		return c;
	}

	public Cacheable get(int posn, Set<BlockPtr> pinnedBlocks) {
		Cacheable c = super.get(posn);
		if (c instanceof BlockPtr) {
			BlockPtr p = (BlockPtr) c;
			if (parentBlock instanceof IndexBlock) {
				return (Cacheable) CacheObjectMgr.getInstance().getIndexBlock(p, parentBlock.getSchemaObjectId(), pinnedBlocks);
			} else {
				return (Cacheable) CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(p, parentBlock.getSchemaObjectId(), pinnedBlocks);
			}
		}
		return c;
	}

	@Override
	public Cacheable getAndPin(int posn, Set<BlockPtr> pinnedBlocks) {
		Cacheable c = getUncached(posn);
		if (c instanceof BlockPtr) {
			BlockPtr p = (BlockPtr) c;
			CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
			return get(posn, pinnedBlocks);
		}
		
//		if (c instanceof BaseBlock) {
//			Block b = (Block) c;
//			CacheEntryPinner.getInstance().pin(b.getPtr(), pinnedBlocks);
//			return b;			
//		}
		return c;
	}
	
//	public void clear() {
//		list.clear();
//	}
}
