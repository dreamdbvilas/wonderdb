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

import java.util.concurrent.atomic.AtomicLong;

import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.server.WonderDBServer;
import org.wonderdb.types.impl.StringType;


public class FileBlockEntryType {
	public static final int BASE_SIZE = Integer.SIZE/8 + Long.SIZE/8 + BlockPtrSerializer.BASE_SIZE;
	
//	public static final int DEFAULT_BLOCK_SIZE = 2048;
	private static final long DEFAULT_FILE_POSN = 0;
	
	public StringType fileName;
	int blockSize;
	AtomicLong currentPosn;
	byte fileId = -1;
	boolean defaultFile = false;
	RecordId recordId = null;
	
	public FileBlockEntryType(String fileName) {
		this(fileName, WonderDBServer.DEFAULT_BLOCK_SIZE, DEFAULT_FILE_POSN);
	}
	
	public FileBlockEntryType(String fileName, int blockSize, long currentPosn) {
		this.fileName = new StringType(fileName);
		this.blockSize = blockSize;
		this.currentPosn = new AtomicLong(currentPosn);
	}
	
	public String getFileName() {
		return fileName.get();
	}
	
	public int getBlockSize() {
		return blockSize;
	}
	
	public long getCurrentPosn() {
		return currentPosn.get();
	}
	
	public long addBy(long p) {
		return currentPosn.getAndAdd(p);
	}
	
	public String getSerializerName() {
		return "fbet";
	}

	public int getByteSize() {
		return BASE_SIZE + fileName.getByteSize();
	}
	
	public byte getFileId() {
		return fileId;
	}
	
	public void setFileId(byte id) {
		this.fileId = id;
	}
	
	public boolean isDefault() {
		return defaultFile;
	}
	
	public void setDefault(boolean flag) {
		defaultFile = flag;
	}
	
	public RecordId getRecordId() {
		return recordId;
	}
	
	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}
}
