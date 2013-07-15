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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.BlockPtrSerializer;


public class SerializedBlockImpl implements SerializedBlock {
	private ChannelBuffer fullBuffer = null;
	private ChannelBuffer dataBuffer = null;
	
	transient protected long lastAccessTimestamp = -1;
	protected BlockPtr blockPtr = null;
	protected byte blockType;
	
	// header = 8 - timestamp + 9 BlockPtr + 4 - txn id + 1 - txn end indicator
	// full header = 1 - record type + 4 - reserved 
	
	private static final int BLOCK_PTR_POSN = 0;
	private static final int RESERVED_POSN = BLOCK_PTR_POSN + BlockPtrSerializer.BASE_SIZE; // integer
	private static final int BLOCK_TYPE_POSN = RESERVED_POSN + Integer.SIZE/8; // byte
	public static final int TXN_ID_POSN = BLOCK_TYPE_POSN + 1; // integer
	public static final int TXN_END_INDICATOR_POSN = TXN_ID_POSN + Integer.SIZE/8; // integer
	public static final int HEADER_SIZE = TXN_END_INDICATOR_POSN + 1; // byte
	public static final int SMALL_HEADER_SIZE = BlockPtrSerializer.BASE_SIZE + 1;
	
	protected SerializedBlockImpl() {
	}
	
	public SerializedBlockImpl(BlockPtr ptr, ChannelBuffer... buffers) {
		
		if (buffers != null && ptr != null) {
			ChannelBuffer[] dBuffer = new ChannelBuffer[buffers.length];
			int posn = 0;
			for (ChannelBuffer b : buffers) {
				b.clear();
				if (posn == 0) {
					dBuffer[posn] = b.slice(HEADER_SIZE, b.capacity()-HEADER_SIZE);
					dBuffer[posn].writerIndex(TXN_ID_POSN);
					dBuffer[posn].writeInt(0);
					dBuffer[posn].writerIndex(TXN_END_INDICATOR_POSN);
					dBuffer[posn].writeByte((byte) 0);
				} else {
					dBuffer[posn] = b.slice(SMALL_HEADER_SIZE, b.capacity()-SMALL_HEADER_SIZE);
				}
				dBuffer[posn].clear();
				dBuffer[posn].writerIndex(dBuffer[posn].capacity());
				BlockPtr p = new SingleBlockPtr(ptr.getFileId(), ((posn++ * b.capacity()) + ptr.getBlockPosn()));
				b.writerIndex(BLOCK_PTR_POSN);
				BlockPtrSerializer.getInstance().toBytes(p, b);
				b.writerIndex(b.capacity());
				b.resetReaderIndex();
			}
			blockPtr = ptr;
			fullBuffer = ChannelBuffers.wrappedBuffer(buffers);
			fullBuffer.clear();
			fullBuffer.writerIndex(fullBuffer.capacity());
			fullBuffer.readerIndex(BLOCK_TYPE_POSN);
			blockType = fullBuffer.readByte();
			dataBuffer = ChannelBuffers.wrappedBuffer(dBuffer);
			dataBuffer.clear();
		}		
	}
	
	public void rebuild() {
		
	}
	
	public void updateBlockType() {
		fullBuffer.clear();
		fullBuffer.writerIndex(fullBuffer.capacity());
		fullBuffer.readerIndex(BLOCK_TYPE_POSN);
		blockType = fullBuffer.readByte();
	}
	
	public void invalidateBlockType() {
		fullBuffer.clear();
		fullBuffer.writerIndex(BLOCK_TYPE_POSN);
		fullBuffer.writeByte((byte) -1);
		
	}
	
	@Override
	public TransactionLogSerializedBlock getTransactionLogBLock() {
		fullBuffer.clear();
		fullBuffer.writerIndex(fullBuffer.capacity());
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(this.fullBuffer);
		buffer.clear();
		return new TransactionLogSerializedBlockImpl(buffer);
	}
	
	public SerializedBlockImpl(long ts, byte blockType, BlockPtr ptr, int reserved, ChannelBuffer... buffers) {
		if (buffers != null && ptr != null) {
			ChannelBuffer[] dBuffer = new ChannelBuffer[buffers.length];
			int posn = 0;
			for (ChannelBuffer b : buffers) {
				b.clear();
				if (posn == 0) {
					dBuffer[posn] = b.slice(HEADER_SIZE, b.capacity()-HEADER_SIZE);
				} else {
					dBuffer[posn] = b.slice(SMALL_HEADER_SIZE, b.capacity()-SMALL_HEADER_SIZE);
				}
				dBuffer[posn].clear();
				dBuffer[posn].writerIndex(dBuffer[posn].capacity());
				BlockPtr p = new SingleBlockPtr(ptr.getFileId(), ((posn * b.capacity()) + ptr.getBlockPosn()));
				b.writerIndex(BLOCK_PTR_POSN);
				BlockPtrSerializer.getInstance().toBytes(p, b);
				b.clear();
				b.writerIndex(b.capacity());
				posn++;
			}
			lastAccessTimestamp = ts;
			blockPtr = ptr;
			this.blockType = blockType;
			fullBuffer = ChannelBuffers.wrappedBuffer(buffers);
			fullBuffer.clear();
			BlockPtrSerializer.getInstance().toBytes(ptr, fullBuffer);
			fullBuffer.writeInt(reserved); // reserved bytes
			fullBuffer.writeByte(blockType);
			fullBuffer.writeInt(-1); // txn id
			dataBuffer = ChannelBuffers.wrappedBuffer(dBuffer);
			dataBuffer.clear();
		}
	}
	
	public SerializedBlockImpl(long ts, byte blockType, BlockPtr ptr, ChannelBuffer... buffers) {
		this(ts, blockType, ptr, 0, buffers);
	}	
	
	@Override
	public long getLastAccessTime() {
		return lastAccessTimestamp;
	}
	
	@Override
	public BlockPtr getPtr() {
		return blockPtr;
	}
	
	@Override
	public void setLastAccessTime(long ts) {
		lastAccessTimestamp = ts;
	}
	
	@Override
	public int getReserved() {
		fullBuffer.writerIndex(fullBuffer.capacity());
		fullBuffer.readerIndex(RESERVED_POSN);
		return fullBuffer.readInt();
	}
	
	@Override
	public void setReserved(int val) {
		fullBuffer.resetReaderIndex();
		fullBuffer.writerIndex(RESERVED_POSN);
		fullBuffer.writeInt(val);
	}
	
	@Override
	public ChannelBuffer getDataBuffer() {
		return dataBuffer;
	}
	
	@Override
	public ChannelBuffer getFullBuffer() {
		return fullBuffer;
	}
	
	@Override
	public byte getBlockType() {
		return blockType;
	}

	@Override
	public ChannelBuffer getData() {
		return fullBuffer;
	}

	@Override
	public void clear() {
	}

	@Override
	public int getByteSize() {
		return fullBuffer.capacity();
	}

	@Override
	public int compareTo(BlockPtr o) {
		BlockPtr ptr = getPtr();
		return ptr.compareTo(o);
	}

	@Override
	public int getBufferCount() {
		return StorageUtils.getInstance().getSmallestBlockCount(blockPtr);
	}
//
//	@Override
//	public long getLastModifiedTime() {
//		return lastModifiedTime;
//	}
//
//	@Override
//	public void setLastModifiedTime(long ts) {
//		this.lastModifiedTime = ts;
//	}

	@Override
	public boolean canFullEvict(long ts) {
		return true;
	}

	@Override
	public int evict(long ts) {
		return 0;
	}
	
	@Override
	public synchronized SerializedBlock  buildReference() {
//		return this;
		WrappedSerializedBlock block = new WrappedSerializedBlock(this);
		
		this.fullBuffer.clear();
		this.fullBuffer.writerIndex(this.fullBuffer.capacity());
		block.fullBuffer = ChannelBuffers.wrappedBuffer(this.fullBuffer);
		
		this.dataBuffer.clear();
		this.dataBuffer.writerIndex(this.dataBuffer.capacity());
		block.dataBuffer = ChannelBuffers.wrappedBuffer(this.dataBuffer);
		
		return block;
	}
	
	private class WrappedSerializedBlock implements SerializedBlock {
		ChannelBuffer fullBuffer = null;
		ChannelBuffer dataBuffer = null;
		SerializedBlockImpl block = null;
		
		private WrappedSerializedBlock(SerializedBlockImpl block) {
			this.block = block;
		}

		@Override
		public BlockPtr getPtr() {
			return block.getPtr();
		}

		@Override
		public ChannelBuffer getData() {
			return fullBuffer;
		}

		@Override
		public void clear() {
		}

		@Override
		public long getLastAccessTime() {
			return block.getLastAccessTime();
		}

		@Override
		public void setLastAccessTime(long ts) {
			block.setLastAccessTime(ts);
		}

		@Override
		public int getByteSize() {
			return block.getByteSize();
		}

		@Override
		public int getBufferCount() {
			return block.getBufferCount();
		}

		@Override
		public boolean canFullEvict(long ts) {
			return block.canFullEvict(ts);
		}

		@Override
		public int evict(long ts) {
			return block.evict(ts);
		}

		@Override
		public ExternalReference<BlockPtr, ChannelBuffer> buildReference() {
			throw new RuntimeException("Method not supported");
		}

		@Override
		public int compareTo(BlockPtr arg0) {
			return block.compareTo(arg0);
		}

		@Override
		public byte getBlockType() {
			return block.getBlockType();
		}

		@Override
		public int getReserved() {
			return block.getReserved();
		}

		@Override
		public void setReserved(int val) {
			throw new RuntimeException("Method not supported");
		}

		@Override
		public TransactionLogSerializedBlock getTransactionLogBLock() {
			throw new RuntimeException("Method not supported");
		}

		@Override
		public ChannelBuffer getDataBuffer() {
			return this.dataBuffer;
		}

		@Override
		public ChannelBuffer getFullBuffer() {
			return this.fullBuffer;
		}
	}
}
