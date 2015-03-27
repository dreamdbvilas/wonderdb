package org.wonderdb.types;

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

import org.wonderdb.server.ServerProperties;



public class FileBlockEntry implements DBType {
	public String fileName;
	int blockSize;
	byte fileId = -1;
	boolean defaultFile = false;
	RecordId recordId = null;
	
	public FileBlockEntry() {
	}
	
	public FileBlockEntry(String fileName) {
		this(fileName, ServerProperties.getInstance().getDefaultBlockSize());
	}
	
	public FileBlockEntry(String fileName, int blockSize) {
		this.fileName = fileName;
		this.blockSize = blockSize;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public byte getFileId() {
		return fileId;
	}

	public void setFileId(byte fileId) {
		this.fileId = fileId;
	}

	public boolean isDefaultFile() {
		return defaultFile;
	}

	public void setDefaultFile(boolean defaultFile) {
		this.defaultFile = defaultFile;
	}

	public RecordId getRecordId() {
		return recordId;
	}

	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public int hashCode() {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public boolean equals(Object o) {
		throw new RuntimeException("Method not supported");
	}
}
