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
package org.wonderdb.types;

import org.wonderdb.server.WonderDBPropertyManager;


public class SingleBlockPtr implements BlockPtr, Serializable {
	long posn;
	byte fileId;

	private static int blockSize = WonderDBPropertyManager.getInstance().getDefaultBlockSize();
	
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
	
	@Override
	public BlockPtr clone() {
		return new SingleBlockPtr(fileId, posn);
	}
	
	@Override
	public int compareTo(DBType ptr) {
		BlockPtr p = null;
		if (ptr instanceof BlockPtr) {
			p = (BlockPtr) ptr;
		} else {
			return 1;
		}
		return posn > p.getBlockPosn() ? 1 : posn < p.getBlockPosn() ? -1 : fileId == p.getFileId() ? 0 : fileId < p.getFileId() ? -1 : 1;
	}

	@Override
	public int hashCode() {
		int i = (int) (posn/blockSize)+fileId;
//		long x = posn >> 11;
//		int i = (int) x + fileId;
		if (i == Integer.MIN_VALUE) {
			i = 0;
		}
		return i;
	}
	
	@Override
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

	@Override
	public int getSize() {
		return 1 + Long.SIZE/8;
	}
}
