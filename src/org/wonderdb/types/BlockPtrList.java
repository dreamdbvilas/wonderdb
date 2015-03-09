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
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}
}
