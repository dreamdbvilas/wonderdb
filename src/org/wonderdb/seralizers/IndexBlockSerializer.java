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
package org.wonderdb.seralizers;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.index.IndexLeafBlock;
import org.wonderdb.block.index.impl.disk.DiskIndexBranchBlock;
import org.wonderdb.block.index.impl.disk.DiskIndexLeafBlock;
import org.wonderdb.types.impl.IndexKeyType;


public class IndexBlockSerializer {
	private static final IndexBlockSerializer instance = new IndexBlockSerializer();
	
	private IndexBlockSerializer() {
	}
	
	public static IndexBlockSerializer getInstance() {
		return instance;
	}
	
	public void toBytes(IndexBlock block, ChannelBuffer buffer, int schemaId) {
		if (block instanceof IndexBranchBlock) {
			branchBlockToBytes(block, buffer);
		} else {
			leafBlockToBytes(block, buffer, schemaId);
		}
	}
	
	public IndexBranchBlock unmarshalBranchBlock(BlockPtr ptr, ChannelBuffer buffer) {
		IndexBranchBlock block = new DiskIndexBranchBlock(-1, ptr);
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		
		int size = buffer.readInt();
		for (int i = 0; i < size; i++) {
			BlockPtr p = BlockPtrSerializer.getInstance().unmarshal(buffer);
			block.getData().add(p);
		}
		BlockPtr p = BlockPtrSerializer.getInstance().unmarshal(buffer);
		if (p != null && p.getFileId() < 0) {
			p = null;
		}
		block.setParent(p);
		p = BlockPtrSerializer.getInstance().unmarshal(buffer);
		if (p.getFileId() < 0) {
			p = null;
		}
		block.setNext(p);
		p = BlockPtrSerializer.getInstance().unmarshal(buffer);
		if (p.getFileId() < 0) {
			p = null;
		}
		block.setPrev(p);
		return block;
	}
	
	public IndexLeafBlock unmarshalLeafBlock(BlockPtr ptr, ChannelBuffer buffer, int schemaId) {
		IndexLeafBlock block = new DiskIndexLeafBlock(schemaId, ptr);
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		
		int size = buffer.readInt();
		
		for (int i = 0; i < size; i++) {
			IndexKeyType ikt = IndexSerializer.getInstance().unmarshal(buffer, schemaId);
			block.getData().add(ikt);
		}
		BlockPtr p = BlockPtrSerializer.getInstance().unmarshal(buffer);
		if (p.getFileId() < 0) {
			p = null;
		}
		block.setParent(p);
		p = BlockPtrSerializer.getInstance().unmarshal(buffer);
		if (p.getFileId() < 0) {
			p = null;
		}
		block.setNext(p);
		p = BlockPtrSerializer.getInstance().unmarshal(buffer);
		if (p.getFileId() < 0) {
			p = null;
		}
		block.setPrev(p);
		return block;
	}
	
	private void branchBlockToBytes(IndexBlock block, ChannelBuffer buffer) {
		buffer.clear();
		int size = block.getData().size();
		buffer.writeInt(size);
		for (int i = 0; i < size; i++) {
			BlockPtr p = (BlockPtr) block.getData().getUncached(i);
			BlockPtrSerializer.getInstance().toBytes(p, buffer);
		}
		BlockPtrSerializer.getInstance().toBytes(block.getParent(), buffer);
		BlockPtrSerializer.getInstance().toBytes(block.getNext(), buffer);
		BlockPtrSerializer.getInstance().toBytes(block.getPrev(), buffer);
	}
	
	private void leafBlockToBytes(IndexBlock block, ChannelBuffer buffer, int schemaId) {
		buffer.clear();
		int size = block.getData().size();
		buffer.writeInt(size);
		for (int i = 0; i < size; i++) {
			IndexKeyType ikt = (IndexKeyType) block.getData().getUncached(i);
			IndexSerializer.getInstance().toBytes(ikt, buffer, schemaId);
		}
		BlockPtrSerializer.getInstance().toBytes(block.getParent(), buffer);
		BlockPtrSerializer.getInstance().toBytes(block.getNext(), buffer);
		BlockPtrSerializer.getInstance().toBytes(block.getPrev(), buffer);
	}
}
