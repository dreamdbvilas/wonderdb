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

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;
import org.wonderdb.cache.tasks.WriterTaskFactory;

public class BaseCacheHandler<Data, CacheContent> implements CacheHandler<CacheContent> {
	CacheMap<CacheContent> cacheMap = null;
	CacheLock cacheLock = null;
	InflightReads<CacheContent> inflightReads = null;
	CacheState cacheState = null;
	CacheBean cacheBean = null;
	public CacheResourceProvider<Data> cacheResourceProvider = null;
	CacheEvictor<CacheContent> eagerEvictor = null;
	CacheEvictor<CacheContent> normalEvictor = null;
	
	public BaseCacheHandler(CacheMap<CacheContent> cacheMap, CacheBean cacheBean, CacheState cacheState, 
			CacheLock cacheLock, CacheResourceProvider<Data> cacheResourceProvider, 
			InflightReads<CacheContent> inflightReads,	WriterTaskFactory writerTaskFactory) {
		this.cacheMap = cacheMap;
		this.cacheLock = cacheLock;
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
		this.inflightReads = inflightReads;
		this.cacheResourceProvider = cacheResourceProvider;
		eagerEvictor = new CacheEvictor<CacheContent>(true, cacheBean, cacheState, cacheLock, cacheMap, this);
		normalEvictor = new CacheEvictor<CacheContent>(false, cacheBean, cacheState, cacheLock, cacheMap, this);
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
	public ExternalReference<BlockPtr, CacheContent> getFromCache(BlockPtr ptr) {		
		return cacheMap.get(ptr);
	}
	@Override
	public ExternalReference<BlockPtr, CacheContent> addIfAbsent(ExternalReference<BlockPtr, CacheContent> block) {
		return cacheMap.addIfAbsent(block);
	}
	
	@Override
	public ExternalReference<BlockPtr, CacheContent> get(BlockPtr ptr) {
		if (ptr == null || ptr.getBlockPosn() < 0) {
			return null;
		}
		
		ExternalReference<BlockPtr, CacheContent> block = getFromCache(ptr);
		if (block == null && inflightReads != null) {
			// read it from the disk.
			block = inflightReads.getBlock(ptr);
		}

		return block;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void remove(BlockPtr ptr) {
		if (ptr == null) {
			return;
		}
		
		ExternalReference<BlockPtr, CacheContent> block = cacheMap.evict(ptr);
		cacheResourceProvider.returnResource((Data) block);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public ExternalReference<BlockPtr, CacheContent> removeForEvict(BlockPtr ptr) {
		if (ptr == null) {
			return null;
		}
		
		ExternalReference<BlockPtr, CacheContent> block = cacheMap.evict(ptr);
		cacheResourceProvider.returnResource((Data) block);
		return block;
	}
	
	@Override
	public void changed(ExternalReference<BlockPtr, CacheContent> ref) {
//		ref.setLastModifiedTime(System.currentTimeMillis());
		cacheMap.changed(ref.getPtr());
	}	
	
	@Override
	public void changed(BlockPtr ptr) {
//		ref.setLastModifiedTime(System.currentTimeMillis());
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
}