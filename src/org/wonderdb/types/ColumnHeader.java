package org.wonderdb.types;

public class ColumnHeader {
	boolean isExtended = false;
	boolean isNull = false;
	
	public ColumnHeader() {
	}
	
	public boolean isExtended() {
		return isExtended;
	}
	
	public void setExtended(boolean isExtended) {
		this.isExtended = isExtended;
	}
	
	public boolean isNull() {
		return isNull;
	}
	
	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}
}
