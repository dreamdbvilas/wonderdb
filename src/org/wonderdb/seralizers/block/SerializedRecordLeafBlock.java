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
import org.wonderdb.seralizers.BlockPtrSerializer;


public class SerializedRecordLeafBlock extends SerializedRecordBlock implements SerializedLeafBlock {
	public static final int HEADER_SIZE = 2*BlockPtrSerializer.BASE_SIZE;
	
	public SerializedRecordLeafBlock(SerializedBlock serializedBlock, boolean newBlock) {
		super(serializedBlock, newBlock, HEADER_SIZE);
//		BlockPtr ptr = new SingleBlockPtr( (byte)-1, -1);
//		if (newBlock) {
//			setNextPtr(ptr);
//			setPrevPtr(ptr);
//		}
	}
	
	@Override
	public BlockPtr getNextPtr() {
		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		return BlockPtrSerializer.getInstance().unmarshal(buffer);
	}
	
	@Override
	public void setNextPtr(BlockPtr p) {
		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
		buffer.clear();
		BlockPtrSerializer.getInstance().toBytes(p, buffer);		
	}
	
	public BlockPtr getPrevPtr() {
		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
		buffer.readerIndex(BlockPtrSerializer.BASE_SIZE);
		buffer.writerIndex(buffer.capacity());
		return BlockPtrSerializer.getInstance().unmarshal(buffer);		
	}
	
	public void setPrevPtr(BlockPtr p) {
		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
		buffer.clear();
		buffer.writerIndex(BlockPtrSerializer.BASE_SIZE);
		BlockPtrSerializer.getInstance().toBytes(p, buffer);		
	}
	
}
