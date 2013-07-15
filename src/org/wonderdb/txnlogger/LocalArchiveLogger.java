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
package org.wonderdb.txnlogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.server.WonderDBPropertyManager;

public class LocalArchiveLogger {
	private static LocalArchiveLogger instance = new LocalArchiveLogger();
	
	List<RandomAccessFile> logFiles = new ArrayList<RandomAccessFile>();
	int currentFilePosn = 0;
	int maxFileCount = 10;
	
	private LocalArchiveLogger() {
		String filePath = WonderDBPropertyManager.getInstance().getLogFilePath();
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
	
	public static LocalArchiveLogger getInstance() {
		return instance;
	}
	
	public Map<Integer, Long> getCollectionCounter() {
		Map<Integer, Long> map = new HashMap<Integer, Long>();
		
		for (int i = 0; i < logFiles.size(); i++) {
			RandomAccessFile raf = logFiles.get(i);
			try {
				if (raf.length() == 0) {
					continue;
				}
				raf.getChannel().position(0);
				ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE/8 + Long.SIZE/8);
				int schemaId = buffer.getInt();
				long id = buffer.getLong();
				long l = map.get(schemaId);
				map.put(schemaId, Math.max(id, l));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return map;
	}
	
	public void log(int schemaId, long id, ChannelBuffer buffer) {
		ByteBuffer buf = ByteBuffer.allocate(buffer.capacity() + Long.SIZE/8 + Integer.SIZE/8);
		buf.putInt(buf.capacity());
		buf.putLong(id);
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		buf.put(buffer.toByteBuffer());
		buf.flip();
		FileChannel channel = getFileChannel(schemaId, id, buffer.capacity());
		try {
			channel.write(buf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private FileChannel getFileChannel(int schemaId, long id, int requiredSize) {
		FileChannel channel = logFiles.get(currentFilePosn).getChannel();
		long currentSize = -1;
		try {
			currentSize = channel.position();
		} catch (IOException e) {
		}
		
		if (currentSize + requiredSize < 100000000) {
			return channel;
		} 

		return findOrCreateNewLogFile(schemaId, id);
	}
	
	private void resetCurrentChannel(int schemaId, long id) {
		FileChannel channel = logFiles.get(currentFilePosn).getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE/8 + Long.SIZE/8);
		buffer.putInt(schemaId);
		buffer.putLong(id);
		buffer.flip();
		try {
			channel.position(0);
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private FileChannel findOrCreateNewLogFile(int schemaId, long id) {
		
		if (logFiles.size() < maxFileCount) {			
			// create new one
			RandomAccessFile raf = null;
			try {
				String filePath = WonderDBPropertyManager.getInstance().getLogFilePath();
	
				raf = new RandomAccessFile(filePath+"/arcivelog"+logFiles.size(), "rw");
				logFiles.add(raf);
				currentFilePosn = logFiles.size()-1;
				resetCurrentChannel(schemaId, id);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			currentFilePosn = currentFilePosn + 1;
			currentFilePosn = currentFilePosn % maxFileCount;
			resetCurrentChannel(schemaId, id);
		}
		return logFiles.get(currentFilePosn).getChannel();
	}	
}
