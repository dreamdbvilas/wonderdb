package org.wonderdb.types;

import java.util.List;

public class ExtendedColumn extends Column implements Extended {
	List<BlockPtr> ptrList = null;
	
	public ExtendedColumn(DBType value, List<BlockPtr> list) {
		super(value);
		this.ptrList = list;
	}
	
	@Override
	public List<BlockPtr> getPtrList() {
		return ptrList;
	}

	@Override
	public void setPtrList(List<BlockPtr> list) {
		this.ptrList = list;
	}
}
