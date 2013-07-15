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
package org.wonderdb.cache;

import java.util.List;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;


public class CacheEvictor<Data> implements Runnable {
	boolean finish = false;
	CacheBean cacheBean = null;
	CacheState cacheState = null;
	CacheLock cacheLock = null;
	boolean eagerCleanup = true;
	CacheEntryPinner pinner = CacheEntryPinner.getInstance();
	CacheMap<Data> cacheMap = null;
	CacheHandler<Data> cacheHandler = null;
	
	public CacheEvictor(boolean eagerCleanup, CacheBean cacheBean, CacheState cacheState, CacheLock cacheLock, CacheMap<Data> cacheMap, CacheHandler<Data> cacheHandler) {
		this.eagerCleanup = eagerCleanup;
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
		this.cacheLock = cacheLock;
		this.cacheMap = cacheMap;
		this.cacheHandler = cacheHandler;
	}
	
	public void finish() {
		finish = true;
		cacheLock.notifyStartCleanup();
		cacheLock.notifyEagerCleanup();
	}
	
	public void run() {
		long lastRunTime = Long.MAX_VALUE;
		int size = cacheMap.listSize;
		long objCountAfterRun = cacheBean.getCleanupLowWaterMark();
		if (eagerCleanup) {
			objCountAfterRun = cacheBean.getCleanupHighWaterMark();
		}
		
		int bucket = 0;
		
//		System.out.println("starting cleanup thread");
		while (true) {
			if (finish) {
//				System.out.println("ending cleanup thread");
				return;
			}
			
			if (eagerCleanup) {
				cacheLock.waitOnEagerCleanup();
			} else {
				cacheLock.waitOnStartCleanup();
			}
			if (finish) {
//				System.out.println("ending cleanup thread");
				return;
			}
//			System.out.println("wait over");
			if (cacheState.getTotalCount() <= objCountAfterRun) {
				continue;
			}
			
//			System.out.println("Starting eviction");
			while (true) {
				List<ExternalReference<BlockPtr, Data>> list = cacheMap.getBucket(bucket, true);
//				bucket = ++bucket % size;
				try {
					for (int j = 0; j < list.size(); j++) {
						ExternalReference<BlockPtr, Data> e = list.get(j);
						if (!pinner.isPinned(e.getPtr()) && !cacheMap.isChanged(e.getPtr())) {
							if (eagerCleanup) {
								ExternalReference<BlockPtr, Data> ref = cacheHandler.removeForEvict(e.getPtr());
								if (pinner.isPinned(e.getPtr()) || cacheMap.isChanged(e.getPtr())) {
									cacheMap.addIfAbsent(ref);
								}
							} else {
								if (e.getLastAccessTime() < lastRunTime) {
									ExternalReference<BlockPtr, Data> ref = cacheHandler.removeForEvict(e.getPtr());
									if (pinner.isPinned(e.getPtr()) || cacheMap.isChanged(e.getPtr())) {
										cacheMap.addIfAbsent(ref);
									}
								}
							}
						}
					}
				} finally {
					cacheMap.returnBucket(bucket, true);
					bucket = ++bucket % size;
				}

				if (cacheState.getTotalCount() <= objCountAfterRun) {
					break;
				}
				lastRunTime = System.currentTimeMillis();
			}			
		}
	}
}
