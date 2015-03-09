package org.wonderdb.types;

import java.util.Map;

public class TableRecordMetadata implements TypeMetadata {
	Map<Integer, Integer> columnIdTypeMap = null;

	public Map<Integer, Integer> getColumnIdTypeMap() {
		return columnIdTypeMap;
	}

	public void setColumnIdTypeMap(Map<Integer, Integer> columnIdTypeMap) {
		this.columnIdTypeMap = columnIdTypeMap;
	}
	
}
