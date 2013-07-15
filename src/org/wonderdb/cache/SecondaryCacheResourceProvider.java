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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.seralizers.SerializedBlockUtils;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.server.WonderDBServer;


public class SecondaryCacheResourceProvider implements CacheResourceProvider<SerializedBlock> {
	private CacheBean cacheBean = null;
	private CacheState cacheState = null;
	private CacheLock cacheLock = null;
	BlockingQueue<ChannelBuffer> freeList = null;
	
	public SecondaryCacheResourceProvider(CacheBean cacheBean, CacheState cacheState, CacheLock cacheLock, int bufferCount) {
		this.cacheBean = cacheBean;
		this.cacheState = cacheState;
		this.cacheLock = cacheLock;
		
		int size = FileBlockManager.getInstance().getSmallestBlockSize();
		
		freeList = new ArrayBlockingQueue<ChannelBuffer>(bufferCount);
		for (int i = 0; i < bufferCount; i++) {
			freeList.add(ChannelBuffers.directBuffer(size));
		}
	}
	
	public SerializedBlock getResource(BlockPtr ptr, byte recordType, long ts) {
		int blockCount = StorageUtils.getInstance().getSmallestBlockCount(ptr);

		ChannelBuffer[] bufferList = new ChannelBuffer[blockCount];
		for (int i = 0; i < blockCount; i++) {
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
			bufferList[i] = buffer;
			if (ts > 0) {
				buffer.writeLong(ts);
				BlockPtrSerializer.getInstance().toBytes(ptr, buffer);
			}
		}
		
		if (cacheState.getTotalCount() >= cacheBean.getCleanupHighWaterMark()) {
			WonderDBServer.writer.startWriting();
			cacheLock.notifyEagerCleanup();
		}
		
		if (cacheState.getTotalCount() >= cacheBean.getCleanupLowWaterMark()) {
			cacheLock.notifyStartCleanup();
		}
		
		return new SerializedBlockImpl(ts, recordType, ptr, bufferList);
	}
	
	@Override
	public void returnResource(SerializedBlock ref) {
		SerializedBlock block = ref;
//		block.invalidateBlockType();
		
		ChannelBuffer[] buffers = SerializedBlockUtils.getInstance().getBuffers(block.getFullBuffer());
		for (int i = 0; i < buffers.length; i++) {
			ChannelBuffer buffer = buffers[i];
			buffer.clear();
			freeList.add(buffer);
			cacheState.updateTotalCountBy(-1);
		}
	}
//
//	@Override
//	public void returnPrimaryResource(BlockPtr ptr) {
//		throw new RuntimeException("method not supported");
//	}
//
//	@Override
//	public void getPrimaryResource(BlockPtr ptr) {
//		throw new RuntimeException("method not supported");
//	}	
}
