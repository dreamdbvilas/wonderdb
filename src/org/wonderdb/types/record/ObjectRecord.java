package org.wonderdb.types.record;

import org.wonderdb.types.DBType;

public interface ObjectRecord extends Record {
	public DBType getColumn();
	public void setColumn(DBType column);
}
