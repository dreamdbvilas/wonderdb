package org.wonderdb.cache;

import java.util.concurrent.Future;

public interface CacheDestinationWriter<Key, Data> {
	public void copy(Data data);
	public Future<Integer> write(Key key);
}
