package org.wonderdb.types;

import java.util.List;

public class IndexRecordMetadata implements TypeMetadata {
	List<Integer> columnIdList = null;
	List<Integer> typeList = null;

	public List<Integer> getTypeList() {
		return typeList;
	}

	public List<Integer> getColumnIdList() {
		return columnIdList;
	}
	
	public void setTypeList(List<Integer> typeList) {
		this.typeList = typeList;
	}

	public void setColumnIdList(List<Integer> columnIdList) {
		this.columnIdList = columnIdList;
	}
}
