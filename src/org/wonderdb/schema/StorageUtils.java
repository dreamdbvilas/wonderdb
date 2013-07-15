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
package org.wonderdb.schema;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.seralizers.block.SerializedRecordBlock;
import org.wonderdb.seralizers.block.SerializedRecordChunk;


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
	
	public int getSmallestBlockCount(String fileName) {
		int blockSize = FileBlockManager.getInstance().getBlockSize(fileName);
		return blockSize/getSmallestBlockSize();		
	}
	
	
//	public int getSmallestBlockCount(int schemaId) {
//		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
//		String fileName = null;
//		if (colMeta == null) {
//			IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex(schemaId);
//			FileBlockManager.getInstance().
//			fileName = idxMeta.getFileName();
//		}
//		return getSmallestBlockCount(fileName);
//	}
	
	
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

	public int getSmallBlockCount(int byteSize) {
		int smallestBlockSize = getSmallestBlockSize();
		int overhead = Long.SIZE/8 + (2*BlockPtrSerializer.BASE_SIZE);
		smallestBlockSize = smallestBlockSize - overhead;
		return new BigDecimal(byteSize/smallestBlockSize).round(new MathContext(2, RoundingMode.UP)).intValue();
	}
	
	public int getBlocksRequired(int smallBlockCount, int byteSize, int smallBlockOverhead, int blockOverhead) {		
		int smallBlockAvailableSpace = getSmallestBlockSize() - smallBlockOverhead;
		int fullBlockSize = (smallBlockCount*smallBlockAvailableSpace) - blockOverhead;
		double d1 = (double) (byteSize/fullBlockSize) + 0.5;
		BigDecimal d = new BigDecimal(d1) ;
		d = d.round(new MathContext(1, RoundingMode.UP));
		int i = d.intValue(); 
		return i == 0 ? 1 : i;
	}
	
	public int getBlocksRequired(BlockPtr ptr, int byteSize, int smallBlockOverhead, int blockOverhead) {
		return getBlocksRequired(getSmallestBlockCount(ptr), byteSize, smallBlockOverhead, blockOverhead);
	}

	
	public int getMaxChunkSize(BlockPtr ptr) {
		int smallBlockCount = getSmallestBlockCount(ptr);
		int chunkOverhead = SerializedBlockImpl.HEADER_SIZE + 
							SerializedRecordBlock.HEADER_SIZE +
							SerializedRecordChunk.HEADER_SIZE;
		int totalSize = getSmallestBlockSize();
		return (totalSize*smallBlockCount) - chunkOverhead;
	}

	public int getMaxChunkSize(byte fileId) {
		int smallBlockCount = getSmallestBlockCount(fileId);
		int chunkOverhead =  SerializedRecordBlock.HEADER_SIZE +
							 SerializedRecordChunk.HEADER_SIZE;
		int totalSize = getSmallestBlockSize();
		return (totalSize*smallBlockCount) - chunkOverhead - ( SerializedBlockImpl.HEADER_SIZE * smallBlockCount);
	}
	
	public int getOverhead(byte fileId) {
		int smallBlockCount = getSmallestBlockCount(fileId);
		
		int chunkOverhead = (SerializedBlockImpl.HEADER_SIZE * smallBlockCount) + 
							SerializedRecordBlock.HEADER_SIZE +
							SerializedRecordChunk.HEADER_SIZE;
		return chunkOverhead;
	}
}
