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
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.thread.WonderDBThreadFactory;
import org.wonderdb.types.FileBlockEntry;

public class FilePointerFactory {
	private Map<Byte, AsynchronousFileChannel> asyncChannelMap = new HashMap<>();
	private Map<Byte, BlockingQueue<FileChannel>> syncChannelMap = new HashMap<>();
	private Map<Byte, RandomAccessFile> fileMap = new HashMap<>();
	
	ExecutorService executor = null;
	private static FilePointerFactory pool = new FilePointerFactory();
	
	private FilePointerFactory() {
		int asyncCoreSize = WonderDBPropertyManager.getInstance().getDiskAsyncWriterThreadPoolSize();
		WonderDBThreadFactory t = new WonderDBThreadFactory("diskAsyncWriter");
		executor = new ThreadPoolExecutor (asyncCoreSize, asyncCoreSize, 5, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(20), t);
	}
	
	public static FilePointerFactory getInstance() {
		return pool;
	}
	
	public void create(FileBlockEntry entry) {
		Path path = Paths.get(entry.getFileName());
		AsynchronousFileChannel afc = null;
		try {
			Set<OpenOption> set = new HashSet<>();
			set.add(StandardOpenOption.READ);
			set.add(StandardOpenOption.WRITE);
			set.add(StandardOpenOption.CREATE);
			
//				afc = AsynchronousFileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, executor);
			afc = AsynchronousFileChannel.open(path, set, executor);
			asyncChannelMap.put(entry.getFileId(), afc);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		BlockingQueue<FileChannel> q1 = new ArrayBlockingQueue<>(5);
		syncChannelMap.put(entry.getFileId(), q1);
		for (int i = 0; i < 5; i++) {
			path = Paths.get(entry.getFileName());
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
			if (raf.length() < entry.getBlockSize()*10000) {
				raf.setLength(entry.getBlockSize()*10000);
			}
			fileMap.put(entry.getFileId(), raf);
		} catch (FileNotFoundException e) {
		} catch (IOException e1) {
			
		}
	}
	
	public synchronized void closAll() {
		executor.shutdownNow();
		
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
		return asyncChannelMap.get(fileId);
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
