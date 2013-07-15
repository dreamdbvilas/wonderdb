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
package org.wonderdb.block.impl.base;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.types.DBType;

public class SingleBlockPtr implements BlockPtr {
	long posn;
	byte fileId;
	private static final int SIZE = 1 + Long.SIZE/8;
	
	public String getSerializerName() {
		return "bs";
	}
	
	public int getByteSize() {
		return SIZE; 
	}
	
	public SingleBlockPtr(byte fileId, long posn) {
		this.posn = posn;
		this.fileId = fileId;
	}
	
	public long getBlockPosn() {
		return posn;
	}
	
	public byte getFileId() {
		return fileId;
	}
	
	public int compareTo(DBType ptr) {
		if (ptr instanceof SingleBlockPtr) {
			BlockPtr p = (BlockPtr) ptr;
			return posn > p.getBlockPosn() ? 1 : posn < p.getBlockPosn() ? -1 : fileId == p.getFileId() ? 0 : fileId < p.getFileId() ? -1 : 1;
		}
		return 1;
	}

	public int hashCode() {
		int i = (int) posn+fileId+Integer.MAX_VALUE;
		if (i == Integer.MIN_VALUE) {
			i = 0;
		}
		return i;
	}
	
	public boolean equals(Object o) {
		if (o instanceof SingleBlockPtr) {
			return this.compareTo((SingleBlockPtr) o) == 0;
		}
		return false;
	}
	
	public String toString() {
		return posn + "_" + fileId;
	}
	
	public BlockPtr copyOf() {
		return new SingleBlockPtr(this.getFileId(), this.getBlockPosn());
	}	
}
