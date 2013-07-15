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

import org.wonderdb.block.BlockPtr;
import org.wonderdb.seralizers.BlockPtrSerializer;

class SerializedIndexLeafBlock {
	public static final int HEADER_SIZE = 3*BlockPtrSerializer.BASE_SIZE;
	
	public SerializedIndexLeafBlock(SerializedBlock serializedBlock, boolean newBlock) {
//		super(serializedBlock, newBlock, HEADER_SIZE);
		if (newBlock) {
//			createNewPosn(1, 0, LEAF_BLOCK);
		}
	}
	
//	@Override
	public BlockPtr getNextPtr() {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		buffer.writerIndex(buffer.capacity());
//		return BlockPtrSerializer.getInstance().unmarshal(buffer);
		return null;
	}
	
//	@Override
	public void setNextPtr(BlockPtr p) {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		BlockPtrSerializer.getInstance().toBytes(p, buffer);		
	}

//	@Override
//	public BlockPtr getPrevPtr() {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		buffer.writerIndex(buffer.capacity());
//		buffer.readerIndex(2*BlockPtrSerializer.BASE_SIZE);
//		return BlockPtrSerializer.getInstance().unmarshal(buffer);
//	}
	
//	@Override
//	public void setPrevPtr(BlockPtr p) {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		buffer.writerIndex(2*BlockPtrSerializer.BASE_SIZE);
//		BlockPtrSerializer.getInstance().toBytes(p, buffer);		
//	}

//	@Override
//	public BlockPtr getParentPtr() {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		buffer.writerIndex(buffer.capacity());
//		buffer.readerIndex(BlockPtrSerializer.BASE_SIZE);
//		return BlockPtrSerializer.getInstance().unmarshal(buffer);		
//	}
	
//	@Override
//	public void setParentPtr(BlockPtr ptr) {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		buffer.writerIndex(BlockPtrSerializer.BASE_SIZE);
//		BlockPtrSerializer.getInstance().toBytes(ptr, buffer);				
//	}
	
//	@Override
//	public ChannelBuffer getDataBuffer() {
//		return super.getDataBuffer();
//	}
}
