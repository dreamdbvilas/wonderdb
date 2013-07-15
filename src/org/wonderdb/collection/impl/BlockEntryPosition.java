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
package org.wonderdb.collection.impl;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;

public class BlockEntryPosition implements Comparable<BlockEntryPosition> {
	private BlockPtr blockPtr = null;
	private int posn = -1;
	Block b = null;
	
	// use this only if block is available.
	public BlockEntryPosition(Block b, int p) {
		if (b != null) {
			blockPtr = b.getPtr();
		}
		this.b = b;
		posn = p;
	}
	
	// normally, use this constructor when block is not available.
	public BlockEntryPosition(BlockPtr ptr, int p) {
		blockPtr = ptr;
		posn = p;
	}
	
	public BlockEntryPosition copyOf() {
		BlockEntryPosition bep = new BlockEntryPosition(this.b, this.posn);
		bep.b = this.b;
		return bep;
	}
	
	public Block getBlock() {
		return b;
	}
	
	public BlockPtr getBlockPtr() {
		return blockPtr;
	}
	
	public int getPosn() {
		return posn;
	}
	
	public void setBlock(Block block) {
		if (block == null) {
			blockPtr = null;
			b = null;
			return;
		}
		blockPtr = block.getPtr();
		b = block;
	}
	
	public void setPosn(int posn) {
		this.posn = posn;
	}
	
	public boolean equals(Object o) {
		if (o instanceof BlockEntryPosition) {
			return this.compareTo((BlockEntryPosition) o) == 0;
		}
		return false;
	}
	
	public int compareTo(BlockEntryPosition bep) {
		if (bep == null) {
			return 1;
		}
		BlockPtr ptr = blockPtr;
		int c = ptr.compareTo(bep.blockPtr);
		if (c == 0) {
			return posn > bep.getPosn() ? 1 : posn < bep.getPosn() ? -1 : 0;
		}
		
		return c;
	}
}
