package org.wonderdb.cache;


public interface Cacheable<Key, Data> extends Comparable<Key> {
	void setLastAccessTime(long time);
	long getLastAccessTime();
	Key getPtr();
	Data getData();
	Data getFull();
}
