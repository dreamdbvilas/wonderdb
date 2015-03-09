package org.wonderdb.cache.impl;

import org.wonderdb.cache.CacheResourceProvider;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.cache.InflightReads;

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

public class BaseCacheHandler<Key, Data> implements CacheHandler<Key, Data> {
	MemoryCacheMap<Key, Data> cacheMap = null;
	CacheLock cacheLock = null;
//	InflightReads<Key, Data> inflightReads = null;
	CacheState cacheState = null;
	CacheBean cacheBean = null;
	public CacheResourceProvider<Key, Data> cacheResourceProvider = null;
	CacheEvictor<Key, Data> eagerEvictor = null;
	CacheEvictor<Key, Data> normalEvictor = null;
	
	public BaseCacheHandler(MemoryCacheMap<Key, Data> cacheMap, CacheBean cacheBean, CacheState cacheState, 
			CacheLock cacheLock, CacheResourceProvider<Key, Data> cacheResourceProvider, 
			InflightReads<Key, Data> inflightReads) {
		this.cacheMap = cacheMap;
		this.cacheLock = cacheLock;
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
//		this.inflightReads = inflightReads;
		this.cacheResourceProvider = cacheResourceProvider;
		eagerEvictor = new CacheEvictor<Key, Data>(true, cacheBean, cacheState, cacheLock, cacheMap, this);
		normalEvictor = new CacheEvictor<Key, Data>(false, cacheBean, cacheState, cacheLock, cacheMap, this);
		Thread eagerCleanupThread = null;
		eagerCleanupThread = new Thread(eagerEvictor, "Eager Cleanup Thread");
		eagerCleanupThread.start();
		Thread normalCleanupThread = null;
		normalCleanupThread = new Thread(normalEvictor, "Normal Cleanup Thread");
		normalCleanupThread.start();
		
	}
	
	public long getBlockCount() {
		return cacheState.getTotalCount();
	}
	
	@Override
	public Cacheable<Key, Data> getFromCache(Key ptr) {		
		return cacheMap.get(ptr);
	}
	@Override
	public Cacheable<Key, Data> addIfAbsent(Cacheable<Key, Data> block) {
		return cacheMap.addIfAbsent(block);
	}
	
	@Override
	public Cacheable<Key, Data> get(Key ptr) {
		if (ptr == null) {
			return null;
		}
		
		Cacheable<Key, Data> block = getFromCache(ptr);
		return block;
	}
	
	@Override
	public void remove(Key ptr) {
		if (ptr == null) {
			return;
		}
		
		Cacheable<Key, Data> block = cacheMap.evict(ptr);
		cacheResourceProvider.returnResource(block);
	}
	
	@Override
	public Cacheable<Key, Data> removeForEvict(Key ptr) {
		if (ptr == null) {
			return null;
		}
		
		Cacheable<Key, Data> block = cacheMap.evict(ptr);
		cacheResourceProvider.returnResource(block);
		return block;
	}
	
	@Override
	public void changed(Cacheable<Key, Data> ref) {
//		ref.setLastModifiedTime(System.currentTimeMillis());
		cacheMap.changed(ref.getPtr());
	}	
	
	@Override
	public void changed(Key ptr) {
		cacheMap.changed(ptr);
	}	
	
	@Override
	public void clear() {
		cacheMap.clear();
	}

	@Override
	public void shutdown() {
		eagerEvictor.finish();
		normalEvictor.finish();
	}

	@Override
	public void forceAdd(Cacheable<Key, Data> ref) {
		cacheMap.forceAdd(ref);
	}
}