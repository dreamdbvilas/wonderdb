package org.wonderdb.types.record;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;

public class TableRecord implements ListRecord  {
	Map<Integer, DBType> columnMap = null;
	RecordId recordId = null;
	
	public TableRecord(Map<Integer, DBType> map) {
		columnMap = map;
	}
	
	public RecordId getRecordId() {
		return recordId;
	}

	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}

	public Map<Integer, DBType> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(Map<Integer, DBType> columnMap) {
		this.columnMap = columnMap;
	}
	
	public DBType getValue(int colId) {
		DBType dt = columnMap.get(colId);
		if (dt instanceof ExtendedColumn) {
			return ((ExtendedColumn) dt).getValue(null);
		}
		return dt;
	}
	
	@Override
	public DBType copyOf() {
//		throw new RuntimeException("Method not supported");
		TableRecord record = new TableRecord(columnMap);
		record.setRecordId(recordId);
		record.columnMap = new HashMap<>(columnMap);
		return record;
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getResourceCount() {
		int size = 0;
		Iterator<DBType> iter = columnMap.values().iterator();
		while (iter.hasNext()) {
			DBType column = iter.next();
			if (column != null && column instanceof ExtendedColumn) {
				size = size + ((Extended) column).getPtrList().size();
			}
		}
		return size;
	}

}
