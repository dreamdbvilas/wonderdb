package org.wonderdb.types.record;

import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;

public class ObjectListRecord implements ListRecord, ObjectRecord {
	RecordId recordId = null;
	DBType column = null;
	
	public ObjectListRecord(DBType column) {
		this.column = column;
	}
	
	public RecordId getRecordId() {
		return recordId;
	}
	
	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}
	
	public DBType getColumn() {
		return column;
	}
	
	public void setColumn(DBType column) {
		this.column = column;
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Method not supported");
	}	
	
	@Override
	public int hashCode() {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean equals(Object o) {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public int getResourceCount() {
		int size = 0;
		if (column != null && column instanceof ExtendedColumn) {
			size = ((Extended) column).getPtrList().size();
		}
		return size;
	}
}
