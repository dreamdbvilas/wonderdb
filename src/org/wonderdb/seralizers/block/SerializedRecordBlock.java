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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.schema.StorageUtils;


public class SerializedRecordBlock implements SerializedBlock {
	// overhead = 8
	public static final int HEADER_SIZE = Integer.SIZE/8 + // chunk count
										  Integer.SIZE/8; // current record chunk posn
	boolean rebuild = false;
	List<SerializedRecordChunk> blockContentList = null;
	ChannelBuffer freeBuffer = null;
	int currentMaxPosn = -1;
	SerializedBlock serializedBlock = null;
	int extendHeaderBy = -1;
	
	public SerializedRecordBlock(SerializedBlock block, boolean newBlock, int extendHeaderBy) {
		ChannelBuffer fullBuffer = block.getDataBuffer();
		serializedBlock = block;
		if (block.getBlockType() == SerializedBlock.DATA_LEAF_BLOCK) {
			this.extendHeaderBy = 0;
		} else {
			this.extendHeaderBy = SerializedRecordLeafBlock.HEADER_SIZE;
		}
//		this.extendHeaderBy = SerializedRecordLeafBlock.HEADER_SIZE;
		if (!newBlock) {
			loadContents(fullBuffer);
		} else {
			fullBuffer.clear();
			freeBuffer = fullBuffer.slice(HEADER_SIZE+this.extendHeaderBy,fullBuffer.capacity()-HEADER_SIZE-this.extendHeaderBy);
			fullBuffer.writeInt(0);
			fullBuffer.writeInt(0);
			this.currentMaxPosn = 0;
			blockContentList = new ArrayList<SerializedRecordChunk>();
		}		
	}
	
	public ChannelBuffer getExtendedHeaderBuffer() {
		ChannelBuffer fullBuffer = serializedBlock.getDataBuffer();
		return fullBuffer.slice(HEADER_SIZE, extendHeaderBy);
	}
	
	public boolean shouldRebuild() {
		return rebuild;
//		return true;
	}
	
	public boolean buildDataBuffer() {
		if (rebuild) {
			ChannelBuffer[] buffers = new ChannelBuffer[blockContentList.size()];
			for (int i = 0; i < buffers.length; i++) {
				buffers[i] = blockContentList.get(i).getFullBuffer();
				buffers[i].clear();
				buffers[i].writerIndex(buffers[i].capacity());
			}
			ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buffers);
			buffer.clear();
			buffer.writerIndex(buffer.capacity());
			
			ChannelBuffer copiedBuffer = ChannelBuffers.copiedBuffer(buffer);
			copiedBuffer.clear();
			copiedBuffer.writerIndex(copiedBuffer.capacity());
			ChannelBuffer fullBuffer = serializedBlock.getDataBuffer();
			fullBuffer.clear();
			fullBuffer.writeInt(blockContentList.size());
			fullBuffer.writeInt(currentMaxPosn);
			fullBuffer.writerIndex(fullBuffer.writerIndex()+extendHeaderBy);
			fullBuffer.writeBytes(copiedBuffer);			
			
			loadContents(fullBuffer);
			rebuild = false;
			return true;
		}
		return false;
	}
	
	public int getByteSize() {
		return serializedBlock.getDataBuffer().capacity()-freeBuffer.capacity();
	}
	
	
	public ChannelBuffer get(int recPosn) {
		SerializedRecordChunk src = getChunk(recPosn);
		return src != null ? src.getDataBuffer() : null;
	}
	
	public SerializedRecordChunk getChunk(int recPosn) {
		int p = getBlockContentListPosn(recPosn);
		if (p >= 0) {
			return blockContentList.get(p);
		}
		return null;		
	}
	
	public int getChunkCount() {
		return blockContentList.size();
	}
	
	private ChannelBuffer reduceBy(int recPosn, int reduceBy) {
		int p = getBlockContentListPosn(recPosn);
		if (p < 0) {
			return null;
		}
		
		SerializedRecordChunk sbc = getChunk(recPosn);
		int currentSize = sbc.getDataBuffer().capacity();
		ChannelBuffer contentFullBuffer = sbc.getFullBuffer();
		int newSize = currentSize - reduceBy;
		contentFullBuffer = contentFullBuffer.slice(0, newSize+SerializedRecordChunk.HEADER_SIZE);
		ChannelBuffer freeBufferChunk = sbc.getFullBuffer().slice(newSize+SerializedRecordChunk.HEADER_SIZE, reduceBy);
		sbc.updateNewSlice(contentFullBuffer);
		freeBuffer.clear();
		freeBuffer.writerIndex(freeBuffer.capacity());
		freeBufferChunk.clear();
		freeBufferChunk.writerIndex(freeBufferChunk.capacity());
		freeBuffer = ChannelBuffers.wrappedBuffer(freeBufferChunk, freeBuffer);
		if (p != blockContentList.size()-1) {
			// Only for last posn, we dont need to rebuild
			rebuild = true;
		}
		return sbc.getDataBuffer();
	}
		
	public ChannelBuffer resize(int recPosn, int newSize) {
		int p = getBlockContentListPosn(recPosn);
		if (p < 0) {
			return null;
		}
		
		SerializedRecordChunk sbc = blockContentList.get(p);
		int currentSize = sbc.getDataBuffer().capacity();
		int change = currentSize - newSize;
		if (change == 0) {
			return sbc.getDataBuffer();
		}
		if (change > 0) {
			return reduceBy(recPosn, change);
		} 
		return extendBy(recPosn, Math.abs(change));
	}
		
	private ChannelBuffer extendBy(int recPosn, int extendBy) {
		int p = getBlockContentListPosn(recPosn);
		if (p < 0) {
			return null;
		}

		SerializedRecordChunk sbc = blockContentList.get(p);
		ChannelBuffer contentFullBuffer = sbc.getFullBuffer();
		ChannelBuffer extendBuffer = freeBuffer.slice(0, extendBy);
		freeBuffer = freeBuffer.slice(extendBy, freeBuffer.capacity()-extendBy);
		contentFullBuffer.clear();
		contentFullBuffer.writerIndex(contentFullBuffer.capacity());
		extendBuffer.clear();
		extendBuffer.writerIndex(extendBuffer.capacity());
		contentFullBuffer = ChannelBuffers.wrappedBuffer(contentFullBuffer, extendBuffer);
		sbc.updateNewSlice(contentFullBuffer);
		if (p != blockContentList.size()-1) {
			// Only for last posn, we dont need to rebuild
			rebuild = true;
		}
		return sbc.getDataBuffer();
	}
	
	public int getFreeSize() {
		return freeBuffer.capacity();
	}
	
	public void remove(int recPosn) {
		int p = getBlockContentListPosn(recPosn);
		if (p < 0) {
			return;
		}

		SerializedRecordChunk src = null;
		if (p != blockContentList.size()-1) {
			rebuild = true;
		}
//			buildDataBuffer();

		src = blockContentList.remove(p);

		ChannelBuffer removeBuffer = src.getFullBuffer();
//		removeBuffer = removeBuffer.slice(0, removeBuffer.capacity());
		removeBuffer.clear();
		removeBuffer.writerIndex(removeBuffer.capacity());
		ChannelBuffer fullBuffer = serializedBlock.getDataBuffer();
		fullBuffer.clear();
		fullBuffer.writeInt(blockContentList.size());
		freeBuffer.clear();
		freeBuffer.writerIndex(freeBuffer.capacity());
		freeBuffer = ChannelBuffers.wrappedBuffer(removeBuffer, freeBuffer);
	}
	
	public void loadContents(ChannelBuffer buffer) {
		int count = 0;
		int i = 0;
		int size = 0;

		buffer.resetReaderIndex();
		buffer.writerIndex(buffer.capacity());
		
		count = buffer.readInt();
		currentMaxPosn = buffer.readInt(); // current max posn
		buffer.readerIndex(buffer.readerIndex()+extendHeaderBy);
		ArrayList<SerializedRecordChunk> blockContentList1 = new ArrayList<SerializedRecordChunk>(count);
		for (; i < count; i++) {
			size = buffer.readInt();
			if (size > StorageUtils.getInstance().getSmallestBlockSize()) {
				throw new RuntimeException("Invalid chunk size");
			}
			buffer.readerIndex(buffer.readerIndex()-Integer.SIZE/8);
			SerializedRecordChunk sbc = new SerializedRecordChunk(this, buffer.slice(buffer.readerIndex(), size));
			blockContentList1.add(sbc);
			buffer.readerIndex(buffer.readerIndex()+size);
		}
		blockContentList = blockContentList1;

		// free bytes
		freeBuffer = buffer.slice(buffer.readerIndex(), buffer.readableBytes());
	}
	
	public SerializedRecordChunk createNewPosn(int dataSize, long reserved, byte recordType) {
		int headerSize = SerializedRecordChunk.HEADER_SIZE;
		int size = dataSize + headerSize;
		ChannelBuffer buffer = freeBuffer.slice(0, size);
		freeBuffer = freeBuffer.slice(size, freeBuffer.capacity()-size);
		int posn = currentMaxPosn;
		currentMaxPosn++;

		SerializedRecordChunk sbc = new SerializedRecordChunk(this, buffer, posn, reserved, recordType);
		blockContentList.add(sbc);
		ChannelBuffer fullBuffer = serializedBlock.getDataBuffer();
		fullBuffer.clear();
		fullBuffer.writeInt(blockContentList.size());
		fullBuffer.writeInt(currentMaxPosn);
		return sbc;
	}
	
	public int getCurrentMaxPosn() {
		return currentMaxPosn;
	}
	
	private int getBlockContentListPosn(int recPosn) {
		for (int i = 0; i < blockContentList.size(); i++) {
			SerializedRecordChunk sbc = blockContentList.get(i);
			if (sbc.getRecordPosn() == recPosn) {
				return i;
			}
		}
		return -1;		
	}

	public byte getBlockType() {
		return serializedBlock.getBlockType();
	}
	
	public int getReserved() {
		return serializedBlock.getReserved();
	}
	
	public void setReserved(int val) {
		serializedBlock.setReserved(val);
	}
	
	public ChannelBuffer getDataBuffer() {
		throw new RuntimeException("Method not supported");
	}
	
	public ChannelBuffer getFullBuffer() {
		return serializedBlock.getFullBuffer();
	}
	
	@Override
	public TransactionLogSerializedBlock getTransactionLogBLock() {
		return serializedBlock.getTransactionLogBLock();
	}
	
	public SerializedBlock getSerializedBlock() {
		return serializedBlock;
	}

	@Override
	public BlockPtr getPtr() {
		return serializedBlock.getPtr();
	}

	@Override
	public ChannelBuffer getData() {
		return serializedBlock.getData();
	}

	@Override
	public void clear() {
		serializedBlock.clear();
	}

	@Override
	public long getLastAccessTime() {
		return serializedBlock.getLastAccessTime();
	}

	@Override
	public void setLastAccessTime(long ts) {
		serializedBlock.setLastAccessTime(ts);
	}

	@Override
	public int compareTo(BlockPtr o) {
		return serializedBlock.compareTo(o);
	}

	@Override
	public int getBufferCount() {
		return serializedBlock.getBufferCount();
	}
	
	public List<SerializedRecordChunk> getContentList() {
		return blockContentList;
	}

	@Override
	public boolean canFullEvict(long ts) {
		return true;
	}

	@Override
	public int evict(long ts) {
		return 0;
	}
	
	
	@Override
	public SerializedBlock buildReference() {
		return this;
	}
}
