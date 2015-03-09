package org.wonderdb.types;

public class NullType implements DBType {
	public static NullType instance = new NullType();
	private NullType() {
	}

	public static NullType getInstance() {
		return instance;
	}
	
	@Override
	public int compareTo(DBType o) {
		if (o == this) {
			return 0;
		}
		return -1;
	}

	@Override
	public DBType copyOf() {
		return instance;
	}

}
