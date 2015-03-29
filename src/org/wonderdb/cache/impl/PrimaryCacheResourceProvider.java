package org.wonderdb.cache.impl;

import java.util.List;

import org.wonderdb.cache.CacheResourceProvider;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.record.Record;

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


public class PrimaryCacheResourceProvider implements CacheResourceProvider<BlockPtr, List<Record>> {
	private CacheBean cacheBean = null;
	private CacheState cacheState = null;
	private CacheLock cacheLock = null;
	
	public PrimaryCacheResourceProvider(CacheBean cacheBean, CacheState cacheState, CacheLock cacheLock) {
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
		this.cacheLock = cacheLock;
		
	}
	
	public void getResource(BlockPtr ptr, int bufferCount) {
		int size = bufferCount;
		
		cacheState.updateTotalCountBy(size);
		if (cacheBean.getCleanupHighWaterMark() <= cacheState.getTotalCount()) {
			cacheLock.notifyStartCleanup();
		}
		if (cacheBean.getCleanupHighWaterMark() <= cacheState.getTotalCount()) {
			cacheLock.notifyEagerCleanup();
		}
	}

	@Override
	public void returnResource(Cacheable<BlockPtr, List<Record>> resource) {
		int size = StorageUtils.getInstance().getSmallestBlockCount(resource.getPtr());
		int s = 1;
		for (int i = 0; i < resource.getData().size(); i++) {
			Record record = resource.getData().get(i);
			s = s + record.getResourceCount();
		}
		cacheState.updateTotalCountBy((-1*s) * size);		
	}
	
	public void returnResource(BlockPtr ptr) {
		int size = StorageUtils.getInstance().getSmallestBlockCount(ptr);
		cacheState.updateTotalCountBy((-1) * size);
	}
}
