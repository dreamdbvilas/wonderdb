package org.wonderdb.types;

public class ColumnSerializerMetadata implements TypeMetadata {
	int columnId = -1;

	public ColumnSerializerMetadata(int id) {
		this.columnId = id;
	}
	
	public int getColumnId() {
		return columnId;
	}

	public void setColumnId(int columnId) {
		this.columnId = columnId;
	}
}
