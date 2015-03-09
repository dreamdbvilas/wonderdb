package org.wonderdb.types.record;

import java.util.List;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;

public class ExtendedObjectListRecord extends ObjectListRecord implements Extended {
	List<BlockPtr> ptrList = null;
	
	public ExtendedObjectListRecord(DBType column, List<BlockPtr> list) {
		super(column);
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
