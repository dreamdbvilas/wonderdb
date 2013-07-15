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

import org.wonderdb.block.CacheableList;

public class PrimaryCacheMap extends CacheMap<CacheableList> {

	public PrimaryCacheMap(int listSize, int concurrencyLevel) {
		super(listSize, concurrencyLevel, false);
		// TODO Auto-generated constructor stub
	}
//	
//	public PrimaryCacheMap(int listSize, CacheBean cacheBean, CacheState cacheState, CacheLock cacheLock,
//			CacheEntryPinner pinner, CacheResourceProvider freeCacheResourceProvider, 
//			InflightReads inflightReads, int concurrencyLevel, CacheMap secondaryCache) {
//		super(listSize, cacheBean, cacheState, cacheLock, pinner, freeCacheResourceProvider, inflightReads, concurrencyLevel, secondaryCache);
//	}
//	
//	public boolean isEvictionSupportedOfChangedBlock() {
//		return false;
//	}
//	
//	public boolean deleteOnFetch() {
//		return false;
//	}
}
