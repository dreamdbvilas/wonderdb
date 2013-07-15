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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.SecondaryCacheResourceProvider;
import org.wonderdb.cache.SecondaryCacheResourceProviderFactory;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.StorageUtils;


public class SerializedMultiBlockChunk {
	
	List<SerializedRecordChunk> chunkList = new ArrayList<SerializedRecordChunk>();
	ChannelBuffer dataBuffer = null;
	long reserved = -1;
	byte chunkType = -1;
	byte fileId = -1;
	Set<BlockPtr> changesBlocks = new HashSet<BlockPtr>();
	SerializedRecord sr = null;
	Set<BlockPtr> pinnedBlocks = null;
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static SecondaryCacheResourceProvider cacheResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();

	public SerializedMultiBlockChunk(int schemaId, SerializedRecordBlock srb, RecordId recordId, Set<BlockPtr> pinnedBlocks, int recordType, SerializedRecord sr) {
		fileId = recordId.getPtr().getFileId();
		this.pinnedBlocks = pinnedBlocks;
		this.sr = sr;
		loadRecord(srb, recordId, recordType);
	}
	
	public SerializedRecordBlock getRecordBlock() {
		return chunkList.size() > 0 ? chunkList.get(0).getRecordBlock() : null;  
	}
	
	public void rebuild() {
//		boolean built = false;
		for (int i = 0; i < chunkList.size(); i++) {
			SerializedRecordChunk src = chunkList.get(i);
//			int posn = src.getRecordPosn();
			
			src.getRecordBlock().buildDataBuffer();
//			if (!built) {
//				built = b;
//			}
//			chunkList.set(i, src.getRecordBlock().getChunk(posn));
		}
//		buildRecord(schemaId, srb, allocatedSize, chunkType, pinnedBlocks);
//		if (built) {
//			ChannelBuffer[] buffers = new ChannelBuffer[chunkList.size()];
//			for (int i = 0; i < chunkList.size(); i++) {
//				buffers[i] = chunkList.get(i).getDataBuffer();
//				buffers[i].clear();
//				buffers[i].writerIndex(buffers[i].capacity());
//			}
//			
//			dataBuffer = ChannelBuffers.wrappedBuffer(buffers);			
//		}
	}
	
	public SerializedMultiBlockChunk(int schemaId, byte fileId, int size, 
			long reserved, byte chunkType, Set<BlockPtr> pinnedBlocks, SerializedRecord sr) {
		this.chunkType = chunkType;
		this.fileId = fileId;
		this.pinnedBlocks = pinnedBlocks;
		this.sr = sr;
		SerializedRecordBlock srb = createNewRecordBlock(schemaId, pinnedBlocks);
		sr.srbMap.put(srb.getPtr(), srb);
		buildRecord(schemaId, srb, size, chunkType, pinnedBlocks);
	}

	public SerializedMultiBlockChunk(int schemaId, SerializedRecordBlock srb, int size, 
			long reserved, byte chunkType, Set<BlockPtr> pinnedBlocks, SerializedRecord sr) {
		this.chunkType = chunkType;
		fileId = srb.getPtr().getFileId();
		this.pinnedBlocks = pinnedBlocks;
		this.sr = sr;
		buildRecord(schemaId, srb, size, chunkType, pinnedBlocks);
	}
	
	
	private void buildRecord(int schemaId, SerializedRecordBlock srb, int allocatedSize, byte chunkType, Set<BlockPtr> pinnedBlocks) {
		SerializedRecordBlock tmpRecordBlock = srb;
		int recordOverhead = SerializedRecordChunk.HEADER_SIZE;
		SerializedRecordChunk tmpChunk = null;
		while (allocatedSize > 0) {
			if (tmpRecordBlock.getFreeSize() < recordOverhead) {
				tmpRecordBlock = createNewRecordBlock(schemaId, pinnedBlocks);
			}
			int dataSize = Math.min(allocatedSize, tmpRecordBlock.getFreeSize()-recordOverhead);
			
			tmpChunk = tmpRecordBlock.createNewPosn(dataSize, reserved, chunkType);
			if (chunkList.size() > 0) {
				chunkList.get(chunkList.size()-1).setNextRecordId(tmpChunk.getRecordId());
			}
			chunkList.add(tmpChunk);
			allocatedSize = allocatedSize-dataSize;
		}
		ChannelBuffer[] buffer = new ChannelBuffer[chunkList.size()];
		for (int i = 0; i < chunkList.size(); i++) {
			buffer[i] = chunkList.get(i).getDataBuffer();
			buffer[i].resetReaderIndex();
			buffer[i].writerIndex(buffer[i].capacity());
		}
		dataBuffer = ChannelBuffers.wrappedBuffer(buffer);	
	}
	
	
	public RecordId getRecordId() {
		if (chunkList.size() == 0) {
			return null;
		}
		BlockPtr ptr = chunkList.get(0).getRecordBlock().getPtr();
		int posn = chunkList.get(0).getRecordPosn();
		return new RecordId(ptr, posn);
	}
	
	public ChannelBuffer resize(int newDataSize) {
		int currentSize = 0;
		if (chunkList.size() != 0) {
			currentSize = dataBuffer.capacity();
		}
		
		if (newDataSize == currentSize) {
			return dataBuffer;
		}
		
		if (newDataSize < currentSize) {
			return reduceNewSize(currentSize, currentSize-newDataSize);
		}
		
		if (newDataSize > currentSize) {
			return increaseNewSize(currentSize, newDataSize-currentSize);
		}
		return null;
	}
	
	public int getFreeSize() {
		if (chunkList.size() == 0) {
			return 0;
		}
		
		SerializedRecordChunk src = chunkList.get(chunkList.size()-1);
		return src.getRecordBlock().getFreeSize();
	}
	
	public ChannelBuffer getDataBuffer() {
		return dataBuffer;
	}
		
	public SerializedRecordBlock getFirstRecordBlock() {
		if (chunkList.size() == 0) {
			return null;
		}
		return chunkList.get(0).getRecordBlock();
	}

	public SerializedRecordBlock getLastRecordBlock() {
		if (chunkList.size() == 0) {
			return null;
		}
		return chunkList.get(chunkList.size()-1).getRecordBlock();
	}
	
	public List<BlockPtr> getChangedBlocks() {
		List<BlockPtr> list = new ArrayList<BlockPtr>(changesBlocks);
		if (chunkList == null) {
			return list;
		}
		
		for (int i = 0; i < chunkList.size(); i++) {
			SerializedRecordChunk src = chunkList.get(i);
//			src.getRecordBlock().buildDataBuffer();
			list.add(src.getRecordBlock().getPtr());
		}
		return list;
	}
	
	public int getChangedBlockCount() {
		return chunkList != null ? chunkList.size() : 0;
	}
	
	public void remove(BlockPtr rootBlockPtr) {
		if (chunkList == null || chunkList.size() == 0) {
			return;
		}
		
		for (int i = 0; i < chunkList.size(); i++) {
			SerializedRecordChunk src = chunkList.get(i);
			if (i == 0) {
				changesBlocks.add(src.getRecordBlock().getPtr());
			}
			src.getRecordBlock().remove(src.getRecordPosn());
			SerializedRecordBlock srb = src.getRecordBlock();
			if (!srb.getPtr().equals(rootBlockPtr) && srb.getChunkCount() == 0) {
				cacheResourceProvider.returnResource(src.getRecordBlock());
			}
		}
		chunkList.clear();
	}
	
	private ChannelBuffer reduceNewSize(int currentSize, int reduceBy) {
		if (chunkList.size() == 0) {
			return null;
		}
		
		List<SerializedRecordChunk> removeList = new ArrayList<SerializedRecordChunk>();
		
		for (int i = chunkList.size()-1; i >= 0; i--) {
			SerializedRecordChunk src = chunkList.get(i);
			int cap = src.getDataBuffer().capacity();
			if (i == 0 || reduceBy <= cap) {
				int cSize = src.getDataBuffer().capacity();
				src.getRecordBlock().resize(src.getRecordPosn(), cSize - reduceBy);
				reduceBy = 0;
				RecordId recId = src.getRecordId();
				recId = new RecordId(recId.getPtr(), -1);
				src.setNextRecordId(recId);
				break;
			} else {
					src.getRecordBlock().remove(src.getRecordPosn());
					removeList.add(src);
					SerializedRecordChunk prevChunk = i-1  >= 0 ? chunkList.get(i-1) : null;
					if (prevChunk != null) {
						RecordId recId = prevChunk.getNextRecordId();
						recId = new RecordId(recId.getPtr(), -1);
						prevChunk.setNextRecordId(recId);
					} else {
						RecordId recId = src.getRecordId();
						recId = new RecordId(recId.getPtr(), -1);
						src.setNextRecordId(recId);
					}
					reduceBy = reduceBy - cap;
			}
		}
		
		chunkList.removeAll(removeList);
		for (int i = 0; i < removeList.size(); i++) {
			SerializedRecordChunk src = removeList.get(i);
			if (src.getRecordBlock().getChunkCount() == 0) {
				cacheResourceProvider.returnResource(src.getRecordBlock().getSerializedBlock());
			}
		}
		
		ChannelBuffer[] buffer = new ChannelBuffer[chunkList.size()];
		for (int i = 0; i < chunkList.size(); i++) {
			buffer[i] = chunkList.get(i).getDataBuffer();
			buffer[i].resetReaderIndex();
			buffer[i].writerIndex(buffer[i].capacity());
		}
		dataBuffer = ChannelBuffers.wrappedBuffer(buffer);
		return dataBuffer;
	}
	
	private ChannelBuffer increaseNewSize(int currentSize, int increaseBy) {
		SerializedRecordChunk currentChunk = null;
		SerializedRecordChunk prevChunk = null;
		int maxChunkSize = StorageUtils.getInstance().getMaxChunkSize(fileId);
		if (chunkList.size() != 0) {
			currentChunk = chunkList.get(chunkList.size()-1);
		}

		int freeSize = currentChunk == null ? 0 : currentChunk.recordBlock.getFreeSize();
		int dataSize = 0;
		
		do {
			freeSize = currentChunk.getRecordBlock().getFreeSize();
			int currentChunkSize = currentChunk.getDataBuffer().capacity();
			if (freeSize > 0) {
				dataSize = Math.min(freeSize, increaseBy);
				currentChunk.getRecordBlock().resize(currentChunk.getRecordPosn(), (currentChunkSize + dataSize));
				increaseBy = increaseBy-dataSize;
			} else {
				prevChunk = currentChunk;
				
				long filePosn = FileBlockManager.getInstance().getNextBlock(sr.getRecordId().getPtr());
				BlockPtr nextPtr = new SingleBlockPtr(fileId, filePosn);
				SerializedBlock ref = cacheResourceProvider.getResource(nextPtr, SerializedBlock.DATA_LEAF_BLOCK, System.currentTimeMillis());
				CacheEntryPinner.getInstance().pin(ref.getPtr(), pinnedBlocks);
				secondaryCacheHandler.addIfAbsent(ref);
				SerializedRecordBlock currentBlock = new SerializedRecordBlock(ref, true, 0);
				dataSize = Math.min(increaseBy, maxChunkSize);
				currentChunk = currentBlock.createNewPosn(dataSize, 0, SerializedBlock.DATA_LEAF_BLOCK);
				increaseBy = increaseBy - dataSize;
				chunkList.add(currentChunk);
				prevChunk.setNextRecordId(currentChunk.getRecordId());
				sr.srbMap.put(currentBlock.getPtr(), currentBlock);
			}
			
		} while (increaseBy > 0);
		
		ChannelBuffer[] buffers = new ChannelBuffer[chunkList.size()];
		for (int i = 0; i < chunkList.size(); i++) {
			buffers[i] = chunkList.get(i).getDataBuffer();
			buffers[i].clear();
			buffers[i].writerIndex(buffers[i].capacity());
		}
		
		dataBuffer = ChannelBuffers.wrappedBuffer(buffers);
		return dataBuffer;
	}
	
	private void loadRecord(SerializedRecordBlock srb1, RecordId recordId, int recordType) {
		try {
			List<ChannelBuffer> list = new ArrayList<ChannelBuffer>();
	//		boolean start = true;
			RecordId currentRecordId = recordId;
			int count = 0;
			
			SerializedRecordBlock srb = srb1;
			while (currentRecordId != null) {
				count++;
				if (count >= 100) {
					throw new RuntimeException("Document too long");
				}
				CacheEntryPinner.getInstance().pin(currentRecordId.getPtr(), pinnedBlocks);
				if (srb == null || !currentRecordId.getPtr().equals(srb.getPtr())) {
					if (sr.srbMap.containsKey(currentRecordId.getPtr())) {
						srb = sr.srbMap.get(currentRecordId.getPtr());
					} else {
						SerializedBlock serializedBlock = (SerializedBlock) secondaryCacheHandler.get(currentRecordId.getPtr());
						
						srb = new SerializedRecordBlock(serializedBlock, false, 0);
						sr.srbMap.put(srb.getPtr(), srb);
					}
	//				if (start && recordType != SerializedRecordChunk.DATA_ONLY_BLOCK) {
	//					srb = new SerializedRecordBlock(serializedBlock, false, SerializedRecordLeafBlock.HEADER_SIZE);
	//				} else {
	//					srb = new SerializedRecordBlock(serializedBlock, false, 0);
	//				}
	//
	//				start = false;
				}
				SerializedRecordChunk recordChunk = srb.getChunk(currentRecordId.getPosn());
				chunkList.add(recordChunk);
				currentRecordId = recordChunk.getNextRecordId();
				if (currentRecordId.getPosn() < 0) {
					reserved = currentRecordId.getPtr().getBlockPosn();
					currentRecordId = null;
				}
				recordChunk.getDataBuffer().clear();
				recordChunk.getDataBuffer().writerIndex(recordChunk.getDataBuffer().capacity());
				list.add(recordChunk.getDataBuffer());
			}
			
			ChannelBuffer[] bufArray = new ChannelBuffer[list.size()];
			bufArray = list.toArray(bufArray);
			dataBuffer = ChannelBuffers.wrappedBuffer(bufArray);
			dataBuffer.resetReaderIndex();
			dataBuffer.writerIndex(dataBuffer.capacity());
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	private SerializedRecordBlock createNewRecordBlock(int schemaId, Set<BlockPtr> pinnedBlocks) {
		SerializedBlock ref = null;
		boolean newBlock = true;
		
		long filePosn = FileBlockManager.getInstance().getNextBlock(sr.getRecordId().getPtr());
		BlockPtr ptr = new SingleBlockPtr(fileId, filePosn);
		ref = cacheResourceProvider.getResource(ptr, SerializedBlock.DATA_LEAF_BLOCK, System.currentTimeMillis());
		CacheEntryPinner.getInstance().pin(ref.getPtr(), pinnedBlocks);
		secondaryCacheHandler.addIfAbsent(ref);

//		if (chunkList.size() > 0) {
//			long filePosn = FileBlockManager.getInstance().getNextBlock(fileId);
//			BlockPtr ptr = new SingleBlockPtr(fileId, filePosn);
//			ref = cacheResourceProvider.getResource(ptr, SerializedBlock.DATA_LEAF_BLOCK, System.currentTimeMillis());
//			CacheEntryPinner.getInstance().pin(ref.getPtr(), pinnedBlocks);
//			secondaryCacheHandler.addIfAbsent(ref);
//		} else {
//			RecordBlock rb = null;
////			ref = cacheResourceProvider.getResource(ptr, SerializedBlock.LEAF_BLOCK, System.currentTimeMillis());
//			if (chunkType == SerializedRecordChunk.HEADER_CHUNK) {
//				rb = CollectionTailMgr.getInstance().getBlock(schemaId, pinnedBlocks);
//				ref = (SerializedBlock) secondaryCacheHandler.get(rb.getPtr());
//				newBlock = false;
//			} else {
//				long filePosn = FileBlockManager.getInstance().getNextBlock(fileId);
//				BlockPtr ptr = new SingleBlockPtr(fileId, filePosn);
//				ref = cacheResourceProvider.getResource(ptr, SerializedBlock.DATA_LEAF_BLOCK, System.currentTimeMillis());				
//				CacheEntryPinner.getInstance().pin(ref.getPtr(), pinnedBlocks);
//				secondaryCacheHandler.addIfAbsent(ref);
//			}
//		}
//		CacheEntryPinner.getInstance().pin(ref.getPtr(), pinnedBlocks);
		SerializedRecordBlock srb = new SerializedRecordLeafBlock(ref, newBlock);
		sr.srbMap.put(srb.getPtr(), srb);
		return srb;
	}
}
