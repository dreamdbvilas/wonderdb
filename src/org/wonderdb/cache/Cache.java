package org.wonderdb.cache;



public interface Cache<Key, Data> {

	public abstract Cacheable<Key, Data> addIfAbsent(Cacheable<Key, Data> ref);

	public abstract Cacheable<Key, Data> get(Key bp);

	public abstract void clear();
	
	public abstract Cacheable<Key, Data> evict(Key bp);

	public abstract void changed(Key key);
	
	public abstract void forceAdd(Cacheable<Key, Data> ref);
	
}