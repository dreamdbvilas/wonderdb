package org.wonderdb.types.record;

import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;

public class IndexRecord implements ObjectRecord {
	DBType column = null;

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
	public int getResourceCount() {
		int size = 0;
		if (column != null && column instanceof ExtendedColumn) {
			size = ((Extended) column).getPtrList().size();
		}
		return size;
	}
}
