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

import java.util.Set;

import org.wonderdb.block.BlockPtr;


public class SerializedIndexBranchContinuationBlock extends SerializedContinuationBlock implements SerializedIndexBlock {
//	public static final int HEADER_SIZE = SerializedContinuationBlock.HEADER_SIZE + BlockPtrSerializer.BASE_SIZE;
	
//	public SerializedIndexBranchContinuationBlock(SerializedBlock block, long reserved, Set<BlockPtr> pinnedBlocks) {
//		super(block, reserved, pinnedBlocks);
//	}
//	
	
	public SerializedIndexBranchContinuationBlock(SerializedBlock block, Set<BlockPtr> pinnedBlocks, boolean newBlock, boolean update) {
		super(block, pinnedBlocks, newBlock, update);
//		this.setParentPtr(new SingleBlockPtr((byte) -1, 0));
	}

	public SerializedIndexBranchContinuationBlock(SerializedBlock block, Set<BlockPtr> pinnedBlocks, boolean update) {
		super(block, pinnedBlocks, false, update);
	}
	
	@Override
	public BlockPtr getParentPtr() {
//		ChannelBuffer buffer = super.getDataBuffer();
//		buffer.clear();
//		buffer.writerIndex(buffer.capacity());
//		return BlockPtrSerializer.getInstance().unmarshal(buffer);
		return null;
	}
	
	
	@Override 
	public void setParentPtr(BlockPtr p) {
//		ChannelBuffer buffer = super.getDataBuffer();
//		buffer.clear();
//		BlockPtrSerializer.getInstance().toBytes(p, buffer);
	}
	
//	@Override
//	public ChannelBuffer getDataBuffer() {
//		ChannelBuffer buffer = super.getDataBuffer();
//		return buffer.slice(HEADER_SIZE, buffer.capacity()-HEADER_SIZE);
//	}
}
