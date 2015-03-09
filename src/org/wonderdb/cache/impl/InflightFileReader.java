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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.cache.InflightReads;
import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.types.BlockPtr;



public class InflightFileReader implements InflightReads<BlockPtr, ChannelBuffer> {
	ConcurrentMap<BlockPtr, BlockPtr> inflightPtrMap = new ConcurrentHashMap<BlockPtr, BlockPtr>();

	private static InflightFileReader instance = new InflightFileReader();
	
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	
	private InflightFileReader() {
	}
	
	public static InflightFileReader getInstance() {
		return instance;
	}
	
	@Override
	public Cacheable<BlockPtr, ChannelBuffer> getBlock(BlockPtr ptr) {
		BlockPtr p = inflightPtrMap.putIfAbsent(ptr, ptr);
		Cacheable<BlockPtr, ChannelBuffer> block = null;
		if (p == null) {
			block = readFromFile(ptr);			
			secondaryCacheHandler.addIfAbsent(block);
			inflightPtrMap.remove(ptr);
			synchronized (ptr) {
				ptr.notifyAll();
			}
		} else {
			block = secondaryCacheHandler.getFromCache(p);
				synchronized (p) {
					while (block == null) {
						try {
							p.wait(10);
							break;
						} catch (InterruptedException e) {
						}
					block = secondaryCacheHandler.getFromCache(p);
				}
			}
		}
		
		return block;
	}

	private Cacheable<BlockPtr, ChannelBuffer> readFromFile(BlockPtr p) {
		ByteBuffer buffer = null;
		SerializedBlockImpl block = (SerializedBlockImpl) secondaryResourceProvider.getResource(p, StorageUtils.getInstance().getSmallestBlockCount(p));
		block.getFullBuffer().clear();
		block.getFullBuffer().writerIndex(block.getFullBuffer().capacity());
		buffer = ByteBuffer.allocate(block.getFullBuffer().capacity());
		try {
			FilePointerFactory.getInstance().readChannel(p.getFileId(), p.getBlockPosn(), buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		block.getFullBuffer().clear();
		buffer.flip();
		block.getFullBuffer().writeBytes(buffer);
		return block;	
	}
}
