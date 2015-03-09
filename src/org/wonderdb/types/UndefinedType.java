package org.wonderdb.types;

public class UndefinedType implements DBType {

	private static UndefinedType instance = new UndefinedType();
	
	private UndefinedType() {
	}

	public static UndefinedType getInstance() {
		return instance;
	}
	
	@Override
	public int compareTo(DBType o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DBType copyOf() {
		// TODO Auto-generated method stub
		return null;
	}

}
