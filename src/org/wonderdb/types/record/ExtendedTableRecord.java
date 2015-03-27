package org.wonderdb.types.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;

public class ExtendedTableRecord extends TableRecord implements Extended {
	List<BlockPtr> ptrList = null;

	public ExtendedTableRecord(Map<Integer, DBType> columnMap, List<BlockPtr> list) {
		super(columnMap);
		ptrList = list;
	}
	
	@Override
	public List<BlockPtr> getPtrList() {
		return ptrList;
	}

	@Override
	public void setPtrList(List<BlockPtr> list) {
		this.ptrList = list;
	}
	
	@Override
	public DBType copyOf() {
		ExtendedTableRecord etr = new ExtendedTableRecord(columnMap, ptrList);
		etr.columnMap = new HashMap<Integer, DBType>(columnMap);
		etr.ptrList = new ArrayList<BlockPtr>(ptrList);
		etr.setRecordId(recordId);
		return etr;
	}
	
	public int getResourceCount() {
		int s = super.getResourceCount();
		return s+ptrList.size();
	}
}
