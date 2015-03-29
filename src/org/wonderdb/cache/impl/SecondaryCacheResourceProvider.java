package org.wonderdb.cache.impl;

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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.CacheResourceProvider;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.types.BlockPtr;


public class SecondaryCacheResourceProvider implements CacheResourceProvider<BlockPtr, ChannelBuffer> {
	private CacheBean cacheBean = null;
	private CacheState cacheState = null;
	private CacheLock cacheLock = null;
	private BlockingQueue<ChannelBuffer> freeList = null;
	private int bufferSize = 0;
	private CacheWriter<BlockPtr, ChannelBuffer> cacheWriter = null;
	
	public SecondaryCacheResourceProvider(CacheWriter<BlockPtr, ChannelBuffer> cacheWrtier, CacheBean cacheBean, 
			CacheState cacheState, CacheLock cacheLock, int bufferCount, int bufferSize, CacheWriter<BlockPtr, ChannelBuffer> writer) {
		this.cacheWriter = writer;
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
		this.cacheLock = cacheLock;
		this.bufferSize = bufferSize;
		
		freeList = new ArrayBlockingQueue<ChannelBuffer>(bufferCount);
		for (int i = 0; i < bufferCount; i++) {
			freeList.add(ChannelBuffers.directBuffer(bufferSize));
		}
	}
	
	public Cacheable<BlockPtr, ChannelBuffer> getResource(BlockPtr ptr, int resourceCount) {

		ChannelBuffer[] bufferList = new ChannelBuffer[resourceCount];
		for (int i = 0; i < resourceCount; i++) {
			ChannelBuffer buffer;
			while (true) {
				try {
					buffer = freeList.take();
					break;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			cacheState.updateTotalCountBy(1);
			buffer.clear();
			buffer.writerIndex(buffer.capacity());
			bufferList[i] = buffer;
		}
		
		if (cacheState.getTotalCount() >= cacheBean.getCleanupHighWaterMark()) {
//			cacheWriter.forceStartWriting();
			cacheLock.notifyEagerCleanup();
		}
		
		if (cacheState.getTotalCount() >= cacheBean.getCleanupHighWaterMark()) {
			cacheLock.notifyStartCleanup();
		}
		
		SerializedBlockImpl buf = new SerializedBlockImpl(ptr, bufferList);
		buf.getFull().clear();
		return buf;
	}
	
	public ChannelBuffer[] getBuffers(ChannelBuffer buffer) {
		int size = 0;
		ChannelBuffer[] retVal = new ChannelBuffer[buffer.capacity()/bufferSize]; 
		for (int i = 0; i < retVal.length; i++) {
			ChannelBuffer cb = buffer.slice(size, bufferSize);
			size = size + bufferSize;
			retVal[i] = cb;
		}
		return retVal;
	}

	@Override
	public void returnResource(Cacheable<BlockPtr, ChannelBuffer> ref) {
		if (ref == null) {
			return;
		}
		Cacheable<BlockPtr, ChannelBuffer> block = ref;
		
		ChannelBuffer[] buffers = getBuffers(block.getFull());
		for (int i = 0; i < buffers.length; i++) {
			ChannelBuffer buffer = buffers[i];
			buffer.clear();
			freeList.add(buffer);
			cacheState.updateTotalCountBy(-1);
		}
	}
}
