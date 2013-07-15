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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedBlockImpl;


public class InflightFileReader implements InflightReads<ChannelBuffer> {
	ConcurrentMap<BlockPtr, BlockPtr> inflightPtrMap = new ConcurrentHashMap<BlockPtr, BlockPtr>();

	private static InflightFileReader instance = new InflightFileReader();
	
	private static SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
//	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	
	private InflightFileReader() {
	}
	
	public static InflightFileReader getInstance() {
		return instance;
	}
	
	@Override
	public ExternalReference<BlockPtr, ChannelBuffer> getBlock(BlockPtr ptr) {
		BlockPtr p = inflightPtrMap.putIfAbsent(ptr, ptr);
		ExternalReference<BlockPtr, ChannelBuffer> block = null;
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
//		CacheResourceProvider<SerializedBlock> secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
		CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
		try {
			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
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
		} finally {
			CacheEntryPinner.getInstance().unpin(ptr, pinnedBlocks);
		}
		
		return block;
	}

	private ExternalReference<BlockPtr, ChannelBuffer> readFromFile(BlockPtr p) {
		ByteBuffer buffer = null;
		SerializedBlock block = (SerializedBlock) secondaryResourceProvider.getResource(p, (byte) 0, System.currentTimeMillis());
		block.getFullBuffer().clear();
		block.getFullBuffer().writerIndex(block.getFullBuffer().capacity());
//		buffer = block.getFullBuffer().toByteBuffer();
		buffer = ByteBuffer.allocate(block.getFullBuffer().capacity());
		String fileName = FileBlockManager.getInstance().getFileName(p);
		try {
			FilePointerFactory.getInstance().readChannel(fileName, p.getBlockPosn(), buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		block.getFullBuffer().clear();
		buffer.flip();
		block.getFullBuffer().writeBytes(buffer);
		((SerializedBlockImpl) block).updateBlockType();
		return block;	
	}
}
