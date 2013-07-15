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
package org.wonderdb.block.record.impl.base;

import java.util.Collections;
import java.util.Comparator;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.impl.base.BaseBlock;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.types.Cacheable;


public abstract class BaseRecordBlock extends BaseBlock implements RecordBlock {
	int currentRecordPosn = 0;
	BlockPtr nextPtr = null;
	BlockPtr prevPtr = null;
	@SuppressWarnings("unused")
	private static CacheableComparator comparator = new CacheableComparator();
	
	public BaseRecordBlock(BlockPtr ptr, int schemaObjectId, int currentRecordPosn) {
		super(ptr, schemaObjectId);
		this.currentRecordPosn = currentRecordPosn;
	}
	
	public void updateCurretRecordPosn() {
		currentRecordPosn++;
	}
	
	public QueriableBlockRecord getRecordData(int posn) {
		return (QueriableBlockRecord) binarySearch(getData(), posn, false);
	}
	
	public QueriableBlockRecord removeRecordData(int posn) {
		return (QueriableBlockRecord) getData().remove(posn);
	}
	
	public int getCurrentRecordPosn() {
		return currentRecordPosn;
	}

	public void setNext(BlockPtr blockPtr) {
		if (blockPtr != null && blockPtr.getFileId() < 0 && blockPtr.getBlockPosn() < 0) {
			nextPtr = null;
		} else {
			nextPtr = blockPtr;
		}
	}
	
	public void setPrev(BlockPtr blockPtr) {
		if (blockPtr != null && blockPtr.getFileId() < 0 && blockPtr.getBlockPosn() < 0) {
			prevPtr = null;
		} else {
			prevPtr = blockPtr;
		}
	}
	
	public BlockPtr getNext() {
		return nextPtr;
	}
	
	public BlockPtr getPrev() {
		return prevPtr;
	}
	
//	@Override
//	public int getSmallestBlockCountUsed() {
//		int smallBlockOverhead = Long.SIZE/8 + BlockPtrSerializer.BASE_SIZE;
//		int blockOverhead = BlockSerializer.BASE_SIZE + Integer.SIZE/8 /* current record posn */;
//		int byteSize = getByteSize();
//		int blocksUsed = StorageUtils.getInstance().getBlocksRequired(getPtr(), byteSize, smallBlockOverhead, blockOverhead);
//		int smallestBlockCount = StorageUtils.getInstance().getSmallestBlockCount(getPtr());
//		return blocksUsed * smallestBlockCount;
//	}

	private static QueriableBlockRecord binarySearch(CacheableList list, int posn, boolean remove) {
		int retVal = -1;	
		QueriableBlockRecord rec = null;
		boolean found = false;
		
		for (retVal = 0; retVal < list.size(); retVal++) {
			QueriableBlockRecord e = (QueriableBlockRecord) list.get(retVal);
			if (posn == e.getRecordId().getPosn()) {
				found = true;
				break;
			}
		}
		
		if (found) {
			if (remove) {
				rec = (QueriableBlockRecord) list.remove(retVal);
			} else {
				rec = (QueriableBlockRecord) list.get(retVal);
			}
		}
		return rec;
	}
	
	@Override
	public int evict(long ts) {
		int blockCount = 1;
		for (int i = 0; i < getData().size(); i++) {
			Cacheable st = getData().getUncached(i);
			if (st instanceof BaseRecord) {
				BaseRecord record = (BaseRecord) st;
				if (record.getLastAccessDate() > ts) {
					int size = record.getByteSize();
					blockCount = blockCount + StorageUtils.getInstance().getSmallBlockCount(size);
					record.getColumns().clear();
				}
			}
		}
		return blockCount;
	}
	
	@Override
	public boolean canFullEvict(long ts) {
		return false;
	}
	
	@Override
	public int getBufferCount() {
		int size = 1;
		for (int i = 0; i < getData().size(); i++) {
			QueriableBlockRecord record = (QueriableBlockRecord) getData().get(i);
			size = size + record.getBufferCount();
		}
		return size;
	}
	
	@Override
	public int addEntry(int posn, Cacheable c) {
		CacheableList list = getData();
//		int addPosn = posn;
//		QueriableBlockRecord currentRecord = (QueriableBlockRecord) c;
//		Collections.b
//		if (list.size() <= posn || posn < 0) {
//			list.add(c);
//			addPosn = list.size()-1;
//		} else {
//			QueriableBlockRecord qbr = (QueriableBlockRecord) list.get(posn);
//			boolean forwardDirection = currentRecord.getRecordId().getPosn() > qbr.getRecordId().getPosn();
//			if (forwardDirection) {
//				for (addPosn = addPosn+1; addPosn < list.size(); addPosn++) {
//					qbr = (QueriableBlockRecord) list.get(addPosn);
//					if (currentRecord.getRecordId().getPosn() < qbr.getRecordId().getPosn()) {
//						list.add(addPosn, c);
//						break;
//					}
//				}
//			} else {
//				for (addPosn = addPosn-1; addPosn >=0; addPosn--) {
//					qbr = (QueriableBlockRecord) list.get(addPosn);
//					if (currentRecord.getRecordId().getPosn() > qbr.getRecordId().getPosn()) {
////						list.add(addPosn, c);
//						break;
//					}
//				}				
//				addPosn = Math.max(addPosn, 0);
//				if (list.size() > addPosn+1) {
//					list.add(addPosn+1, c);
//				} else {
//					list.add(addPosn, c);
//				}
//			}
//		}
		int addPosn = Collections.binarySearch(list, c, new CacheableComparator());
		addPosn = addPosn+1;
		list.add(Math.abs(addPosn), c);
		return Math.abs(addPosn);
	}
	
	private static class CacheableComparator implements Comparator<Cacheable> {

		@Override
		public int compare(Cacheable o1, Cacheable o2) {
			if (o1 instanceof QueriableBlockRecord && o2 instanceof QueriableBlockRecord) {
				QueriableBlockRecord l = (QueriableBlockRecord) o1;
				QueriableBlockRecord r = (QueriableBlockRecord) o2;
				if (r.getRecordId().getPosn() > l.getRecordId().getPosn()) {
					return -1;
				}
				if (r.getRecordId().getPosn() < l.getRecordId().getPosn()) {
					return 1;
				}
				return 0;
			}
			throw new RuntimeException("Invalid type for o1 or o2");
		}
	}	
}
