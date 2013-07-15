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
package org.wonderdb.txnlogger.slave;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.wonderdb.server.DreamDBPropertyManager;

public class ArchiveLogger {
	List<RandomAccessFile> logFiles = new ArrayList<RandomAccessFile>();
	int currentFilePosn = 0;
	int maxNoOfFiles = 10;
	
	long currentMinLogTime = -1;
	
	public ArchiveLogger() {
		String filePath = DreamDBPropertyManager.getInstance().getLogFilePath();
		while (true) {
			File file = null;
			file = new File(filePath+"/arcivelog"+logFiles.size(), "rw");
			if (file.exists() || logFiles.size() == 0) {
				try {
					RandomAccessFile raf = new RandomAccessFile(file, "rw");
					logFiles.add(raf);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			} else {
				break;
			}
		}		
	}
	
	public void log(long id, ByteBuffer buffer) {
		FileChannel channel = getFileChannel(id, buffer.capacity());
		try {
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private FileChannel getFileChannel(long id, int requiredSize) {
		FileChannel channel = logFiles.get(currentFilePosn).getChannel();
		long currentSize = -1;
		try {
			currentSize = channel.position();
		} catch (IOException e) {
		}
		
		if (currentSize + requiredSize < 100000000) {
			return channel;
		} 

		return findOrCreateNewLogFile(id);
	}
	
	private void resetCurrentChannel(long id) {
		FileChannel channel = logFiles.get(currentFilePosn).getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE/8);
		buffer.putLong(id);
		buffer.flip();
		try {
			channel.position(0);
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private FileChannel findOrCreateNewLogFile(long id) {
		if (logFiles.size() >= maxNoOfFiles) {
			currentFilePosn = (currentFilePosn + 1) % maxNoOfFiles;
			resetCurrentChannel(id);
			
		} else {
			// create new one
			RandomAccessFile raf = null;
			try {
				String filePath = DreamDBPropertyManager.getInstance().getLogFilePath();
	
				raf = new RandomAccessFile(filePath+"/arcivelog"+logFiles.size(), "rw");
				logFiles.add(raf);
				currentFilePosn = logFiles.size()-1;
				resetCurrentChannel(Long.MAX_VALUE);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		return logFiles.get(currentFilePosn).getChannel();
	}
	
//	public void shutdown() {
//		for (int i = 0; i < logFiles.size(); i++) {
//			try {
//				logFiles.get(i).setLength(0);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}		
//	}	
}
