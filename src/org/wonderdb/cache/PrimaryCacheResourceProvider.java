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

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.schema.StorageUtils;


public class PrimaryCacheResourceProvider implements CacheResourceProvider<Block> {
	private CacheBean cacheBean = null;
	private CacheState cacheState = null;
	private CacheLock cacheLock = null;
	
	public PrimaryCacheResourceProvider(CacheBean cacheBean, CacheState cacheState, CacheLock cacheLock) {
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
		this.cacheLock = cacheLock;
		
	}
	
	public void getResource(BlockPtr ptr, int bufferCount) {
		int size = StorageUtils.getInstance().getSmallestBlockCount(ptr) * bufferCount;
		
		cacheState.updateTotalCountBy(size);
		if (cacheBean.getCleanupLowWaterMark() <= cacheState.getTotalCount()) {
			cacheLock.notifyStartCleanup();
		}
		if (cacheBean.getCleanupHighWaterMark() <= cacheState.getTotalCount()) {
			cacheLock.notifyEagerCleanup();
		}
	}

	public void returnResource(BlockPtr ptr, int bufferCount) {
		int size = StorageUtils.getInstance().getSmallestBlockCount(ptr) * bufferCount;
		cacheState.updateTotalCountBy((-1) * size);
	}

	@Override
	public void returnResource(Block resource) {
		if (resource == null) {
			return;
		}
		if (resource instanceof RecordBlock) {
			returnRecordBlock((RecordBlock) resource);
		} else {
			returnIndexBlock((IndexBlock) resource);
		}
	}
	
	private void returnRecordBlock(RecordBlock block) {
		int size = 1;
		for (int i = 0; i < block.getData().size(); i++) {
			QueriableBlockRecord record = (QueriableBlockRecord) block.getData().get(i);
			size = size + record.getBufferCount();
		}
		
		returnResource(block.getPtr(), size);
	}
	
	private void returnIndexBlock(IndexBlock block) {
		returnResource(block.getPtr(), block.getBufferCount());
	}
}
