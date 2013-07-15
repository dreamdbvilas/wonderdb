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
package org.wonderdb.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class FilePointerFactory {
	private Map<String, RandomAccessFile> fileMap = new HashMap<String, RandomAccessFile>();
	private Map<String, RandomAccessFile> channelMap = new HashMap<String, RandomAccessFile>();
	private static FilePointerFactory pool = new FilePointerFactory();
	
	private FilePointerFactory() {
	}
	
	public static FilePointerFactory getInstance() {
		return pool;
	}
	
	private synchronized RandomAccessFile getRawFilePointer(String fileName) {
		RandomAccessFile file = fileMap.get(fileName);
		if (file == null) {
			try {
				file = new RandomAccessFile(fileName, "rw");
				fileMap.put(fileName, file);
			} catch (IOException e) {
			}
		}
		return file;
	}
	
	
	public synchronized void close(String fileName, RandomAccessFile file) {
		if (file == null || fileName == null || fileName.length() == 0) {
			return;
		}
		try {
			file.close();
		} catch (IOException e) {
		}
		fileMap.remove(fileName);
	}
	
	private FileChannel getChannel(String fileName) {
		RandomAccessFile file = channelMap.get(fileName);
		if (file == null) {
			synchronized (this) {
				if (!channelMap.containsKey(file)) {
					try {
						file = new RandomAccessFile(fileName, "rw");
					} catch (IOException e) {
					}
					channelMap.put(fileName, file);
				}
			}
		}
		return file.getChannel();
	}
	
//	public byte[] readFile(RandomAccessFile file, int count) {
//		byte[] bytes = new byte[count];
//		int posn = 0;
//		int readCount = 0;
//		while (readCount != count) {
//			byte[] b = new byte[count];
//			try {
//				int c = file.read(b);
//				if (c < 0) {
//					break;
//				}
//				readCount = readCount + c;
//				for (int i = 0; i < c; i++) {
//					bytes[posn++] = b[i]; 
//				}
//			} catch (IOException e) {
//			}
//		}
//		return bytes;
//	}
//	
	public void writeFile(String fileName, long posn, ByteBuffer buffer) throws IOException {
		RandomAccessFile file = getRawFilePointer(fileName);
		file.seek(posn);
		file.write(buffer.array());
	}
	
	public void writeChannel(String fileName, long posn, ByteBuffer buffer) throws IOException {
		FileChannel channel = getChannel(fileName);
		channel.write(buffer, posn);
		channel.force(true);
	}
	
	public void readChannel(String fileName, long posn, ByteBuffer buffer) throws IOException {
		readChannel(fileName, posn, 0, buffer);
	}
	
	public void readChannel(String fileName, long posn, long sleep, ByteBuffer buffer) throws IOException {
		if (sleep > 0) {
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
			}
		}
		FileChannel channel = getChannel(fileName);
		channel.read(buffer, posn);
	}

	public long getSize(String fileName) {
		RandomAccessFile file = getRawFilePointer(fileName);
		long len = -1;
		try {
			len = file.length();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return len;
	}
	
	public void doubleSize(String fileName) {
		RandomAccessFile file = getRawFilePointer(fileName);
		try {
			long len = file.length();
			if (len >= 10000000000L) {
				len = len + 10000000000L;
			} else if (len >= 500000) {
				len = len * 2;
			} else {
				len = 500000;				
			}
			file.setLength(len);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
