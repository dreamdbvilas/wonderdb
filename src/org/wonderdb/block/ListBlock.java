package org.wonderdb.block;

import java.util.Collections;
import java.util.Comparator;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.Record;

public class ListBlock extends BaseBlock {
	private static ListBlockComparator cmp = new ListBlockComparator();
	
	int maxPosn = 0;
	public ListBlock(BlockPtr ptr) {
		super(ptr);
	}
	
	public int getMaxPosn() {
		return maxPosn;
	}
	
	public void setMaxPosn(int maxPosn) {
		this.maxPosn = maxPosn;
	}
	
	public int getAndIncMaxPosn() {
		return maxPosn++;
	}
	
	public Record getRecord(int id) {
		int p = Collections.binarySearch(getData(), id, cmp);
		if (p >= 0) {
			return getData().get(p);
		}
		return null;
	}
	
	private static class ListBlockComparator implements Comparator<Object> {

		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof ListRecord && o2 instanceof Integer) {
				ListRecord lr1 = (ListRecord) o1;
				int lr2 = (Integer) o2;
				
				if (lr1.getRecordId().getPosn() == lr2) {
					return 0;
				}
				
				if (lr1.getRecordId().getPosn() < lr2) {
					return -1;
				}
				
				if (lr1.getRecordId().getPosn() > lr2) {
					return 1;
				}
			}
			return 1;
		}
		
	}
}
