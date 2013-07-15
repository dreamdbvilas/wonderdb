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
package org.wonderdb.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.thread.ThreadPoolExecutorWrapper;
import org.wonderdb.txnlogger.LogManager;


public class CacheWriter {
	CacheMap<ChannelBuffer> cacheMap = null;
	int writerFrequency = -1;
	Set<Runnable> inflightRunnableTasks = new HashSet<Runnable>();
	Lock threadPoolFullLock = new ReentrantLock();
	Condition threadPoolFullCondition = threadPoolFullLock.newCondition();
	ConcurrentHashMap<BlockPtr, BlockPtr> inflightBlocks = new ConcurrentHashMap<BlockPtr, BlockPtr>();
	Thread thread = null;
	boolean isShutdown = false;
	long syncTime = -1;
	long inprocessSyncTime = -1;
	
	
	public CacheWriter(CacheMap<ChannelBuffer> cacheMap, int writerFrequency) {
		Writer writer = new Writer();
		this.cacheMap = cacheMap;
		this.writerFrequency = writerFrequency;
		thread = new Thread(writer);
		thread.setDaemon(false);
		thread.start();
	}
	
	public long getSyncTime() {
		return syncTime;
	}
	
	public void shutdown() {
		isShutdown = true;
		thread.interrupt();
	}
	
	public void startWriting() {
		thread.interrupt();
	}
	
	private class Writer implements Runnable {
		ThreadPoolExecutorWrapper executor = new ThreadPoolExecutorWrapper(WonderDBPropertyManager.getInstance().getWriterThreadPoolCoreSize(),
				WonderDBPropertyManager.getInstance().getWriterThreadPoolMaxSize(), 5, 
				WonderDBPropertyManager.getInstance().getWriterThreadPoolQueueSize());
		@Override
		public void run() {
			while (true) {
				try {
					if (writerFrequency < 0) {
						synchronized (cacheMap.writtenBlocks) {
							try {
								cacheMap.writtenBlocks.wait();
							} catch (InterruptedException e) {
							}
						}
					} else {
						try {
							Thread.sleep(writerFrequency);
						} catch (InterruptedException e) {
						}
					}
					
					Iterator<BlockPtr> iter = cacheMap.writtenBlocks.keySet().iterator();
					Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
					inprocessSyncTime = -1;
					
					while (iter.hasNext()) {
						BlockPtr ptr = iter.next();
						cacheMap.getBucket(ptr, true);
						ByteBuffer byteBuffer = null;
						long lastAccessTime = -1;
						try {
							if (!CacheEntryPinner.getInstance().isPinned(ptr) && !inflightBlocks.containsKey(ptr)) {
								CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
								SerializedBlock block = (SerializedBlock) cacheMap.get(ptr);
								
								if (block == null) {
									continue;
								}
								boolean flag = cacheMap.writtenBlocks.replace(ptr, 0, 1);
								if (flag) {
									ChannelBuffer buffer = block.getFullBuffer();
									lastAccessTime = block.getLastAccessTime();
									
									buffer.clear();
									buffer.writerIndex(buffer.capacity());
									ChannelBuffer copiedBuffer = ChannelBuffers.copiedBuffer(buffer);
									copiedBuffer.clear();
									copiedBuffer.writerIndex(copiedBuffer.capacity());
									byteBuffer = copiedBuffer.toByteBuffer();
									flag = cacheMap.writtenBlocks.replace(ptr, 1, 2);
								} else {
									continue;
								}
								if (!flag) {
									continue;
								}
								inflightBlocks.put(ptr, ptr);
							}
						} finally {
							cacheMap.returnBucket(ptr, true);
							CacheEntryPinner.getInstance().unpin(ptr, pinnedBlocks);
						}								
						if (byteBuffer != null) {
							String fileName = FileBlockManager.getInstance().getFileName(ptr);
							byteBuffer.clear();
							
							WriterTask task = new WriterTask(ptr, fileName, byteBuffer, lastAccessTime);
							while (true) {
								try {
									executor.asynchrounousExecute(task);
									break;
								} catch (RejectedExecutionException e) {
									waitForThreadPoolNotFullCondition();
								}
							}
						}
					}
					
					syncTime = Math.max(syncTime, inprocessSyncTime);
					LogManager.getInstance().resetLogs(syncTime);
					if (isShutdown) {
						executor.shutdown();
						return;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void waitForThreadPoolNotFullCondition() {
		try {
			threadPoolFullLock.lock();
			while (true) {
				try {
					threadPoolFullCondition.await(20, TimeUnit.MILLISECONDS);
					break;
				} catch (InterruptedException e) {
				}
			}
		} finally {
			threadPoolFullLock.unlock();
		}
	}
	
	private void notifyThreadPoolNotFull() {
		try {
			threadPoolFullLock.lock();
			threadPoolFullCondition.signalAll();
		} finally {
			threadPoolFullLock.unlock();
		}		
	}
	
	private class WriterTask implements Runnable {
		BlockPtr ptr;
		String fileName;
		ByteBuffer buffer;
		long lastAccessTime = -1;
		
		private WriterTask(BlockPtr ptr, String fileName, ByteBuffer buffer, long lastAccessTime) {
			this.ptr = ptr;
			this.fileName = fileName;
			this.buffer = buffer;
			this.lastAccessTime = lastAccessTime;
		}
		
		@Override
		public void run() {
			try {
				inflightRunnableTasks.add(this);
				FilePointerFactory.getInstance().writeChannel(fileName, ptr.getBlockPosn(), buffer);
				inprocessSyncTime = inprocessSyncTime < 0 ? lastAccessTime : Math.min(lastAccessTime, inprocessSyncTime); 
				inflightRunnableTasks.remove(this);
				cacheMap.writtenBlocks.remove(ptr, 2);
				inflightBlocks.remove(ptr);
				notifyThreadPoolNotFull();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
