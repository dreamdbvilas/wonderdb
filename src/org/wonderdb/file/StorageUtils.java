package org.wonderdb.file;

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

import org.wonderdb.storage.FileBlockManager;
import org.wonderdb.types.BlockPtr;



public class StorageUtils {
	
	private static StorageUtils instance = new StorageUtils();
	
	private StorageUtils() {
	}
	
	public static StorageUtils getInstance() {
		return instance;
	}
	
	public int getSmallestBlockSize() {
		return FileBlockManager.getInstance().getSmallestBlockSize();
	}
	
	public int getSmallestBlockCount(BlockPtr ptr) {
		return getSmallestBlockCount(ptr.getFileId());
	}
	
	public int getSmallestBlockCount(byte fileId) {
		int blockSize = FileBlockManager.getInstance().getBlockSize(fileId);
		return blockSize/getSmallestBlockSize();		
	}
	
	public int getTotalBlockSize(BlockPtr ptr) {
		int smallBlockSize = getSmallestBlockSize();
		int smallBlockCount = getSmallestBlockCount(ptr);
		return smallBlockSize * smallBlockCount;
	}
	
	public int getTotalBlockSize(BlockPtr ptr, int smallBlockOverhead, int blockOverhead) {
		int smallBlockSize = getSmallestBlockSize() - smallBlockOverhead;
		int smallBlockCount = getSmallestBlockCount(ptr);
		return (smallBlockSize * smallBlockCount) - blockOverhead;
	}
}
