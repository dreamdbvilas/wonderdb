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
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.seralizers.BlockPtrSerializer;


public class SerializedRecordChunk {
	ChannelBuffer fullBuffer = null;
	ChannelBuffer dataBuffer = null;
	SerializedRecordBlock recordBlock = null;
	RecordId recordId = null;
	byte chunkType = DATA_CHUNK;
	
	// overhead = 8 (count+posn) + 13 (next rec id) + 1 (start of record chunk) = 22
	public static final int HEADER_SIZE = (2*Integer.SIZE/8) /* count + posn */ + 
							BlockPtrSerializer.BASE_SIZE + Integer.SIZE/8 /* next record id */ +
							1 /* chunk type */;
	
	public static final byte HEADER_CHUNK = (byte) 1;
	public static final byte DATA_CHUNK = (byte) 0;
	public static final byte DATA_ONLY_BLOCK = (byte) 2;
	
	private static final int COUNT_POSN = 0;
	private static final int RECORD_POSN = COUNT_POSN + Integer.SIZE/8;
	private static final int NEXT_BLOCK_PTR_POSN = RECORD_POSN + Integer.SIZE/8;
	
	public SerializedRecordChunk(SerializedRecordBlock recordBlock, ChannelBuffer buffer) {
		this.recordBlock = recordBlock;
		updateNewSlice(buffer);		
	}
	
//	public void rebuild() {
//		recordBlock.buildDataBuffer();
//	}
	
	public SerializedRecordChunk(SerializedRecordBlock recordBlock, ChannelBuffer buffer, int recPosn, 
			long reserved, byte chunkType) {
		fullBuffer = buffer;
		fullBuffer.clear();
		fullBuffer.writeInt(fullBuffer.capacity());
		fullBuffer.writeInt(recPosn);
		BlockPtr p = new SingleBlockPtr(recordBlock.getPtr().getFileId(), reserved);
		BlockPtrSerializer.getInstance().toBytes(p, fullBuffer);
		fullBuffer.writeInt(-1); 
		fullBuffer.writeByte(chunkType);
		this.chunkType = chunkType;
		dataBuffer = fullBuffer.slice(fullBuffer.writerIndex(), fullBuffer.writableBytes());
		this.recordBlock = recordBlock;
		recordId = new RecordId(recordBlock.getPtr(), recPosn);
	}
	
	public void updateNewSlice(ChannelBuffer buffer) {
		fullBuffer = buffer;
		buffer.clear();
		fullBuffer.writeInt(buffer.capacity());
		fullBuffer.readerIndex(fullBuffer.writerIndex());
		fullBuffer.writerIndex(fullBuffer.capacity());
		int posn = fullBuffer.readInt();
		recordId = new RecordId(recordBlock.getPtr(), posn);
		fullBuffer.readerIndex(HEADER_SIZE-1);
		chunkType = fullBuffer.readByte();
		dataBuffer = fullBuffer.slice(fullBuffer.readerIndex(), fullBuffer.readableBytes());
	}
	
	public RecordId getNextRecordId() {
		try {
			fullBuffer.writerIndex(fullBuffer.capacity());
			fullBuffer.readerIndex(NEXT_BLOCK_PTR_POSN);
			BlockPtr p = BlockPtrSerializer.getInstance().unmarshal(fullBuffer);
			int posn = fullBuffer.readInt();
			return new RecordId(p, posn);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public ChannelBuffer getDataBuffer() {
		return dataBuffer;
	}
	
	public int getRecordPosn() {
		fullBuffer.writerIndex(fullBuffer.capacity());
		fullBuffer.readerIndex(RECORD_POSN);
		int p = fullBuffer.readInt();
		return p;
	}
	
	public void setNextRecordId(RecordId recId) {
		fullBuffer.clear();
		fullBuffer.writerIndex(NEXT_BLOCK_PTR_POSN);
		BlockPtrSerializer.getInstance().toBytes(recId.getPtr(), fullBuffer);
		fullBuffer.writeInt(recId.getPosn());
	}	

	public SerializedRecordBlock getRecordBlock() {
		return recordBlock;
	}
	
	public int getOverhead() {
		return HEADER_SIZE;
	}
	
	public ChannelBuffer getFullBuffer() {
		return fullBuffer;
	}	
	
	public RecordId getRecordId() {
		return recordId;
	}
	
	public byte getChunkType() {
		return chunkType;
	}
}
