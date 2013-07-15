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
package org.wonderdb.seralizers.block;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.SecondaryCacheResourceProvider;
import org.wonderdb.cache.SecondaryCacheResourceProviderFactory;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.seralizers.BlockPtrSerializer;


public class SerializedContinuationBlock {
	List<SerializedBlock> list = new ArrayList<SerializedBlock>();
	public static final int HEADER_SIZE = BlockPtrSerializer.BASE_SIZE;
	long reserved = 0;
	Set<BlockPtr> pinnedBlocks = null;
	
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static SecondaryCacheResourceProvider cacheResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	
	public SerializedContinuationBlock(SerializedBlock block, Set<BlockPtr> pinnedBlocks, boolean newBlock, boolean update) {
		this.pinnedBlocks = pinnedBlocks;
		SerializedBlock tmpBlock = block;
		if (update) {
//			tmpBlock.setLastModifiedTime(System.currentTimeMillis());
			secondaryCacheHandler.changed(tmpBlock);
		}
		if (newBlock) {
			list.add(block);
			setNullContPtr(block);
			return;
		}
		
		BlockPtr contPtr = null;
		int count = 0;
		do {
			count++;
			if (contPtr != null) {
				CacheEntryPinner.getInstance().pin(contPtr, pinnedBlocks);
				tmpBlock = (SerializedBlock) secondaryCacheHandler.get(contPtr);
				if (update) {
//					tmpBlock.setLastModifiedTime(System.currentTimeMillis());
					secondaryCacheHandler.changed(tmpBlock);
				}
			}
			list.add(tmpBlock);
			ChannelBuffer fullBuffer = tmpBlock.getDataBuffer();
			fullBuffer.clear();
			fullBuffer.writerIndex(fullBuffer.capacity());
			contPtr = BlockPtrSerializer.getInstance().unmarshal(fullBuffer);
		} while (contPtr.getFileId() >= 0);
	}
	
	public int getTotalDataBufferSize() {
		return (list.get(0).getDataBuffer().capacity()-HEADER_SIZE)*(list.size());
	}	
	
	public ChannelBuffer getDataBuffer() {
		ChannelBuffer[] buffer = new ChannelBuffer[list.size()];
		for (int i = 0; i < list.size(); i++) {
			ChannelBuffer b = list.get(i).getDataBuffer();
			b.clear();
			b.writerIndex(b.capacity());
			buffer[i] = b.slice(HEADER_SIZE, (b.capacity()-HEADER_SIZE));
			buffer[i].clear();
			buffer[i].writerIndex(buffer[i].capacity());
		}
		return ChannelBuffers.wrappedBuffer(buffer);
	}
	
	public ChannelBuffer getDataBuffer(int size) {
		
		int maxSizePerBlock = getTotalDataBufferSize()/list.size();
		double val = (double) size/maxSizePerBlock;
		int blocksRequired = val > (int) val ? (int) val + 1 : (int) val;
		BlockPtr ptr = list.get(0).getPtr();
		byte recordType = list.get(0).getBlockType();
		if (blocksRequired > list.size()) {
			for (int i = 0; i < blocksRequired; i++) {
				if (i < list.size()) {
					CacheEntryPinner.getInstance().pin(list.get(i).getPtr(), pinnedBlocks);
				} else {
					long nextPosn = FileBlockManager.getInstance().getNextBlock(ptr);
					BlockPtr p = new SingleBlockPtr(ptr.getFileId(), nextPosn);
					SerializedBlock tmpBlock = cacheResourceProvider.getResource(p, recordType, System.currentTimeMillis());
					CacheEntryPinner.getInstance().pin(tmpBlock.getPtr(), pinnedBlocks);
					secondaryCacheHandler.addIfAbsent(tmpBlock);
					setBlockPtr(list.get(list.size()-1), tmpBlock.getPtr());
					setNullContPtr(tmpBlock);
					list.add(tmpBlock);
				}
			}
		} else if (blocksRequired < list.size()) {
			int lSize = list.size();
			for (int i = blocksRequired; i < lSize; i++) {
				SerializedBlock block = list.remove(list.size()-1);
				secondaryCacheHandler.remove(block.getPtr());
			}
			setNullContPtr(list.get(list.size()-1));
		}
		
		setNullContPtr(list.get(list.size()-1));
		return getDataBuffer();
	}
	
	private void setBlockPtr(SerializedBlock block, BlockPtr ptr) {
		ChannelBuffer fullBuffer = block.getDataBuffer();
		fullBuffer.clear();
		BlockPtrSerializer.getInstance().toBytes(ptr, fullBuffer);
	}
	
	private void setNullContPtr(SerializedBlock block) {
		ChannelBuffer fullBuffer = block.getDataBuffer();
		fullBuffer.clear();
//		BlockPtr ptr = new SingleBlockPtr( (byte) -1, reserved);
		BlockPtrSerializer.getInstance().toBytes(null, fullBuffer);
	}
	
	public List<SerializedBlock> getBlocks() {
		return list;
	}	
	
	public int getBufferCount() {
		int count = 0;
		for (int i = 0; i < list.size(); i++) {
			count = count + list.get(i).getBufferCount();
		}
		return count;
	}
}
