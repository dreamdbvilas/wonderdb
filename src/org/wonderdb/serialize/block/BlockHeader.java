package org.wonderdb.serialize.block;

public class BlockHeader {
	boolean isIndexBlock = false;
	boolean isIndexBranchBlock = false;
	
	public boolean isIndexBlock() {
		return isIndexBlock;
	}
	
	public void setIndexBlock(boolean isIndexBlock) {
		this.isIndexBlock = isIndexBlock;
	}
	
	public boolean isIndexBranchBlock() {
		return isIndexBranchBlock;
	}
	
	public void setIndexBranchBlock(boolean isIndexBranchBlock) {
		this.isIndexBranchBlock = isIndexBranchBlock;
	}

}
