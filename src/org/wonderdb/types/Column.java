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
}
