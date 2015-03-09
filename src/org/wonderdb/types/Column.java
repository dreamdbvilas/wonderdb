package org.wonderdb.types;





public class Column implements DBType {
	DBType value = null;
	
	protected Column() {
	}
	
	protected Column(DBType value) {
		this.value = value;
	}
	
	public DBType getValue() {
		return value;
	}
	
	public void setValue(DBType o) {
		value = o;
	}

	@Override
	public int compareTo(DBType o) {
		Column c = null;
		if (o instanceof Column) {
			c = (Column) o;
		} else {
			return 1;
		}
		
		return this.value.compareTo(c.value);
	}

	@Override
	public DBType copyOf() {
		return new Column(value.copyOf());
	}
}
