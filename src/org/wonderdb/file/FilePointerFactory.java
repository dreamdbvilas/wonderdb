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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.wonderdb.types.FileBlockEntry;

public class FilePointerFactory {
	private Map<Byte, BlockingQueue<AsynchronousFileChannel>> asyncChannelMap = new HashMap<>();
	private Map<Byte, BlockingQueue<FileChannel>> syncChannelMap = new HashMap<>();
	private Map<Byte, RandomAccessFile> fileMap = new HashMap<>();
	
	private static FilePointerFactory pool = new FilePointerFactory();
	
	private FilePointerFactory() {
	}
	
	public static FilePointerFactory getInstance() {
		return pool;
	}
	
	public void create(FileBlockEntry entry) {
		BlockingQueue<AsynchronousFileChannel> q = new ArrayBlockingQueue<>(5);
		asyncChannelMap.put(entry.getFileId(), q);
		for (int i = 0; i < 5; i++) {
			Path path = Paths.get(entry.getFileName());
			AsynchronousFileChannel afc = null;
			try {
				afc = AsynchronousFileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			q.add(afc);
		}
		
		BlockingQueue<FileChannel> q1 = new ArrayBlockingQueue<>(5);
		syncChannelMap.put(entry.getFileId(), q1);
		for (int i = 0; i < 5; i++) {
			Path path = Paths.get(entry.getFileName());
			FileChannel fc = null;
			try {
				fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			q1.add(fc);
		}
		
		try {
			RandomAccessFile raf = new RandomAccessFile(entry.getFileName(), "rw");
			if (raf.length() < entry.getBlockSize()*100) {
				raf.setLength(entry.getBlockSize()*100);
			}
			fileMap.put(entry.getFileId(), raf);
		} catch (FileNotFoundException e) {
		} catch (IOException e1) {
			
		}
	}
	
	public synchronized void closAll() {
		Iterator<BlockingQueue<AsynchronousFileChannel>> iter = asyncChannelMap.values().iterator();
		while (iter.hasNext()) {
			BlockingQueue<AsynchronousFileChannel> asyncChannels = iter.next();
			AsynchronousFileChannel afc = null;
			for (int closed = 0; closed < 5; closed++) {
				try {
					afc = asyncChannels.take();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				try {
					afc.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		Iterator<BlockingQueue<FileChannel>> syncIter = syncChannelMap.values().iterator();
		while (syncIter.hasNext()) {
			BlockingQueue<FileChannel> syncChannels = syncIter.next();
			for (int closed = 0; closed < 5; closed++) {
				FileChannel fc = null;;
				try {
					fc = syncChannels.take();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				try {
					fc.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public void writeChannel(byte fileId, long posn, ByteBuffer buffer) throws IOException {
		BlockingQueue<FileChannel> q = syncChannelMap.get(fileId);
		FileChannel fc;
		try {
			fc = q.take();
			fc.write(buffer, posn);
			q.add(fc);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readChannel(byte fileId, long posn, ByteBuffer buffer) throws IOException {
		readChannel(fileId, posn, 0, buffer);
	}
	
	public void readChannel(byte fileId, long posn, long sleep, ByteBuffer buffer) throws IOException {
		if (sleep > 0) {
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
			}
		}
		BlockingQueue<FileChannel> q = syncChannelMap.get(fileId);
		FileChannel fc;
		try {
			fc = q.take();
			fc.read(buffer, posn);
			q.add(fc);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	AsynchronousFileChannel getAsyncChannel(byte fileId) {
		BlockingQueue<AsynchronousFileChannel> q = asyncChannelMap.get(fileId);
		try {
			AsynchronousFileChannel c = q.take();
			return c;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	void returnAsyncChannel(byte fileId, AsynchronousFileChannel channel) {
		BlockingQueue<AsynchronousFileChannel> q = asyncChannelMap.get(fileId);
		q.add(channel);
	}
	
	public long increaseSizeBy(byte fileId, long size) {
		RandomAccessFile raf = fileMap.get(fileId);
		long currentSize = -1;
		synchronized (raf) {
			try {
				currentSize = raf.length();
				raf.setLength(currentSize+size);
				long len = raf.length();
				int i = 0;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return currentSize;
	}
//	
//	public void doubleSize(String fileName) {
//		RandomAccessFile file = getRawFilePointer(fileName);
//		try {
//			long len = file.length();
//			if (len >= 10000000000L) {
//				len = len + 10000000000L;
//			} else if (len >= 500000) {
//				len = len * 2;
//			} else {
//				len = 500000;				
//			}
//			file.setLength(len);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public static void main(String[] args) throws Exception {
//		RandomAccessFile raf = new RandomAccessFile("vvv", "rw");
//		RandomAccessFile raf1 = new RandomAccessFile("vvv", "rw");
//		AsynchronousFileChannel afc = AsynchronousFileChannel.open("vvv", StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
//		ByteBuffer buffer = ByteBuffer.allocate(100);
//		buffer.put("vilas".getBytes());
//		raf.getChannel().write(buffer, 0);
//		raf1.getChannel().write(buffer, 2000);
//	}
}
