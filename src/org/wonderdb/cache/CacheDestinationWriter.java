package org.wonderdb.cache;

import java.util.concurrent.Future;

public interface CacheDestinationWriter<Key, Data> {
	public Data copy(Key ptr, Data data);
	public Future<Integer> write(Key key, Data data);
}
