package org.wonderdb.cache.impl;

import org.wonderdb.cache.Cacheable;

/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

public interface CacheHandler<Key, Data> {

	public long getBlockCount();

	public Cacheable<Key, Data> get(Key ptr);
	public Cacheable<Key, Data> addIfAbsent(Cacheable<Key, Data> block);
	public void forceAdd(Cacheable<Key, Data> ref);
	public Cacheable<Key, Data> getFromCache(Key ptr);	
	public void changed(Cacheable<Key, Data> ref);
	public void changed(Key ptr);
	public void remove(Key ptr);
	public Cacheable<Key, Data> removeForEvict(Key pt);
	public void clear();
	public void shutdown();
}