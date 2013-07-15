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


class SerializedIndexBranchBlock  {
	public static final int HEADER_SIZE = BlockPtrSerializer.BASE_SIZE;
	
	public SerializedIndexBranchBlock(SerializedBlock serializedBlock, boolean newBlock) {
//		super(serializedBlock, newBlock, HEADER_SIZE);
	}
	
//	@Override
	public BlockPtr getParentPtr() {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		buffer.writerIndex(buffer.capacity());
//		return BlockPtrSerializer.getInstance().unmarshal(buffer);
		return null;
	}
	
//	@Override
	public void setParentPtr(BlockPtr ptr) {
//		ChannelBuffer buffer = super.getExtendedHeaderBuffer();
//		buffer.clear();
//		BlockPtrSerializer.getInstance().toBytes(ptr, buffer);				
	}
}
