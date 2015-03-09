package org.wonderdb.types;

public interface DBType extends Comparable<DBType>, Cloneable {
	DBType copyOf();
}
