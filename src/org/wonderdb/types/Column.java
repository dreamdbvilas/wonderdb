package org.wonderdb.types;





public class Column implements DBType {
	DBType value = null;
	
	protected Column() {
	}
	
	protected Column(DBType value) {
		this.value = value;
	}
	
//	public DBType getValue() {
//		return value;
//	}
//	
	public void setValue(DBType o) {
		value = o;
	}

	@Override
	public int compareTo(DBType o) {
		DBType oValue = o;
		if (o instanceof Column) {
			oValue = ((Column) o).value;
		}
		
		return this.value.compareTo(oValue);
	}

	@Override
	public DBType copyOf() {
		return new Column(value.copyOf());
	}
	
	@Override
	public int hashCode() {
		return value == null ? 0 : value.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof DBType) {
			return compareTo((DBType) o) == 0;
		}
		return false;
	}
}
