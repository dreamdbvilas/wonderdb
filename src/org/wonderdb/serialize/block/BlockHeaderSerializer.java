package org.wonderdb.serialize.block;

import org.jboss.netty.buffer.ChannelBuffer;

public class BlockHeaderSerializer {
	private static BlockHeaderSerializer instance = new BlockHeaderSerializer();
	
	private BlockHeaderSerializer() {
	}
	
	public static BlockHeaderSerializer getInstance() {
		return instance;
	}
	
	public BlockHeader getHeader(ChannelBuffer buffer) {
		byte treeBlockMask = 0x1;
		byte branchBlockMask = 0x2;

		byte b = buffer.readByte();
		BlockHeader header = new BlockHeader();
		header.setIndexBranchBlock((b & branchBlockMask) > 0);
		header.setIndexBlock((treeBlockMask & b) > 0);
		return header;
	}
	
	public void serialize(BlockHeader header, ChannelBuffer buffer) {
		byte treeBlockMask = 0x1;
		byte branchBlockMask = 0x2;

		byte b = header.isIndexBlock() ? treeBlockMask : 0;
		b =  (byte) (b | (header.isIndexBranchBlock() ? branchBlockMask : 0));
		buffer.writeByte(b);
	}
}
