package org.wonderdb.types;

public class ColumnNameMeta implements DBType {
	int coulmnId;
	String columnName;
	String collectioName;
	int columnType;
	boolean isNull;
	RecordId recId;
	boolean isVirtual = false;
	
	public boolean isVirtual() {
		return isVirtual;
	}

	public void setVirtual(boolean isVirtual) {
		this.isVirtual = isVirtual;
	}

	public int getCoulmnId() {
		return coulmnId;
	}
	
	public void setCoulmnId(int coulmnId) {
		this.coulmnId = coulmnId;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String getCollectioName() {
		return collectioName;
	}
	
	public void setCollectioName(String collectioName) {
		this.collectioName = collectioName;
	}
	
	public int getColumnType() {
		return columnType;
	}
	
	public void setColumnType(int columnType) {
		this.columnType = columnType;
	}
	public boolean isNull() {
	
		return isNull;
	}
	
	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}
	
	public RecordId getRecId() {
		return recId;
	}
	
	public void setRecId(RecordId recId) {
		this.recId = recId;
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Mehtod not supported");
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Mehtod not supported");
	}
	
	@Override
	public int hashCode() {
		throw new RuntimeException("Mehtod not supported");		
	}
	
	@Override
	public boolean equals(Object o) {
		throw new RuntimeException("Mehtod not supported");
	}
}
