package org.wonderdb.types;

import java.util.ArrayList;
import java.util.List;

public class BlockPtrList implements DBType {
	List<BlockPtr> ptrList = new ArrayList<BlockPtr>();
	public BlockPtrList(List<BlockPtr> list) {
		this.ptrList = list;
	}
	
	public List<BlockPtr> getPtrList() {
		return ptrList;
	}
	
	public void setPtrList(List<BlockPtr> ptrList) {
		this.ptrList = ptrList;
	}

	@Override
	public int compareTo(DBType o) {
		BlockPtrList list = null;
		if (o instanceof BlockPtrList) {
			list = (BlockPtrList) o;
		} 
		
		if (list == null) {
			return 1;
		}
		
		if (list.getPtrList() == null && this.ptrList == null) {
			return 0;
		} else if (list.getPtrList() != null && this.ptrList == null) {
			return -1;
		} else {
			if (list.ptrList == null && this.ptrList != null) {
				return 1;
			}
		}
		
		if (list.ptrList.size() > this.ptrList.size()) {
			return -1;
		} else {
			if (list.ptrList.size() < this.ptrList.size()) {
				return 1;
			}
		}
		
		for (int i = 0; i < ptrList.size(); i++) {
			BlockPtr thisPtr = ptrList.get(i);
			BlockPtr thatPtr = list.ptrList.get(i);
			
			if (thatPtr == null && thisPtr == null) {
				continue;
			} else if (thatPtr != null && thisPtr == null) {
				return -1;
			} else if (thatPtr == null && thisPtr != null) {
				return 1;
			}
			int val = thisPtr.compareTo(thatPtr); 
			if ( val == 0) {
				continue;
			} else {
				return val;
			}
		}
		return 0;
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}
	
	@Override 
	public int hashCode() {
		int hashCode = 0;
		if (ptrList == null) {
			return 0;
		}
		for (int i = 0; i < ptrList.size(); i++) {
			BlockPtr ptr = ptrList.get(i);
			hashCode = hashCode + (ptr == null ? 0 : ptr.hashCode());
		}
		return hashCode;
	}
	
	@Override
	public boolean equals(Object o) {
		BlockPtrList list = null;
		if (o instanceof BlockPtrList) {
			list = (BlockPtrList) o;
		}
		
		return this.compareTo(list) == 0;
	}
}
