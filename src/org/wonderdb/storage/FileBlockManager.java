package org.wonderdb.storage;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.server.ServerProperties;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.FileBlockEntry;


public class FileBlockManager {
	
	private static FileBlockManager instance = new FileBlockManager();
	Map<Byte, FileBlockEntry> fileIdToBlockMap = new HashMap<Byte, FileBlockEntry>();
	Map<Byte, BlockingQueue<Long>> pointerPoolMap = new HashMap<Byte, BlockingQueue<Long>>();
	
	byte defaultFileId = -1;
	
	int smallestBlockSize = ServerProperties.getInstance().getDefaultBlockSize();
	
	private FileBlockManager() {
	}
	
	public static FileBlockManager getInstance() {
		return instance;
	}
	
	public List<FileBlockEntry> getFileBlockEntries() {
		return new ArrayList<FileBlockEntry>(fileIdToBlockMap.values());
	}
		
	public FileBlockEntry addFileBlockEntry(FileBlockEntry entry) {
		if (!fileIdToBlockMap.containsKey(entry.getFileId())) {
			fileIdToBlockMap.put(entry.getFileId(), entry);
			if (entry.isDefaultFile() || defaultFileId < 0) {
				defaultFileId = entry.getFileId();
			}
			
			BlockingQueue<Long> bq = new ArrayBlockingQueue<Long>(1000);
			pointerPoolMap.put(entry.getFileId(), bq);
			return entry;
		}
		return fileIdToBlockMap.get(entry.getFileId());
	}
	
	public int getSmallestBlockSize() {
		return smallestBlockSize;
	}
	
	public String getFileName(BlockPtr ptr) {
		FileBlockEntry entry = fileIdToBlockMap.get(ptr.getFileId());
		return entry.getFileName();
	}
	
	public synchronized long getNextBlock(byte fileId) {
		BlockingQueue<Long> bq = pointerPoolMap.get(fileId);
		Long curPosn = null;
		while (true) {
			curPosn = bq.poll();
			if (curPosn == null) {
				FileBlockEntry entry = fileIdToBlockMap.get(fileId);
				int blockSize = entry.getBlockSize();
				int incBy = blockSize * 10;
				long currentSize = FilePointerFactory.getInstance().increaseSizeBy(fileId, incBy);
				for (int i = 0; i < 10; i++) {
					currentSize = currentSize + (i == 0 ? 0 : blockSize);
					bq.add(currentSize);
				}
			} else {
				return curPosn;
			}
		}
	}	
	
	public long getNextBlock(BlockPtr ptr) {
		byte fileId = ptr.getFileId();
		return getNextBlock(fileId);
	}
		
	public int getBlockSize(byte fileId) {
		FileBlockEntry et = fileIdToBlockMap.get(fileId);
		if (et == null) {
			return smallestBlockSize;
		}
		return et.getBlockSize();
	}
	
	public String getDefaultFileName() {
		FileBlockEntry entry = fileIdToBlockMap.get(defaultFileId);
		if (entry == null) {
			return null;
		}
		
		return entry.getFileName();
	}
}
