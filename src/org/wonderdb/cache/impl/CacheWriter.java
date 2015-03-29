package org.wonderdb.cache.impl;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.CacheDestinationWriter;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.txnlogger.LogManager;


public class CacheWriter<Key, Data> extends Thread{	
	MemoryCacheMap<Key, Data> cacheMap = null;
	int writerFrequency = -1;
	Lock threadPoolFullLock = new ReentrantLock();
	Condition threadPoolFullCondition = threadPoolFullLock.newCondition();
	long syncTime = -1;
	long inprocessSyncTime = -1;
	CacheDestinationWriter<Key, Data> writer = null;
	boolean isShutdown = false;
	boolean shutdownOver = false;
	Thread currentThread = null;
	Object shutdownLock = new Object();
	int shutc = 0;
	public CacheWriter(MemoryCacheMap<Key, Data> cacheMap, int writerFrequency, CacheDestinationWriter<Key, Data> writer) {
		this.cacheMap = cacheMap;
		this.writerFrequency = writerFrequency;
		this.writer = writer;
	}
	
	public long getSyncTime() {
		return syncTime;
	}
	
	public void shutdown() {
		isShutdown = true;
//		currentThread.interrupt();
		synchronized (shutdownLock) {
			while(true) {
				try {
					shutdownLock.wait(20);
					if (shutdownOver) {
						break;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
//	public void forceStartWriting() {
//		if (writerFrequency > 0) {
//			currentThread.interrupt();
//		}
//	}
//	
	@Override
	public void run() {
//		currentThread = Thread.currentThread();
		long timeTaken = 0;
		while (true) {
			try {
				if (!isShutdown) {
					if (writerFrequency < 0) {
						synchronized (cacheMap.writtenBlocks) {
							try {
								cacheMap.writtenBlocks.wait();
							} catch (InterruptedException e) {
							}
						}
					} else {
						try {
							if (timeTaken < writerFrequency) {
								Thread.sleep((writerFrequency-timeTaken));
							}
						} catch (InterruptedException e) {
						}
					}
				}
				
				Iterator<Key> iter = cacheMap.writtenBlocks.keySet().iterator();
				Set<Object> pinnedBlocks = new HashSet<Object>();
				inprocessSyncTime = -1;
//				Set<Key> writtenBlocks = new HashSet<Key>();
				Map<Key, Data> bufferMap = new HashMap<Key, Data>();
				
				long start = System.currentTimeMillis();
				while (iter.hasNext()) {
					Key ptr = iter.next();
					cacheMap.getBucket(ptr, true);
					try {
						
//						if (!CacheEntryPinner.getInstance().isPinned(ptr) && cacheMap.writtenBlocks.get(ptr) != 1) {
						if (!CacheEntryPinner.getInstance().isPinned(ptr)) {
							CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
							Cacheable<Key, Data> block = cacheMap.get(ptr);
							if (block == null) {
								continue;
							}
							cacheMap.writtenBlocks.put(ptr, 1);
							Data data = null;
//							if (flag) {								
								data = writer.copy(ptr, block.getFull());
								bufferMap.put(ptr, data);
//								flag = cacheMap.writtenBlocks.replace(ptr, 1, 2);
//							}
//							if (flag) {
//								while (true) {
//									try {
//										writer.write(block.getPtr(), data);
//										break;
//									} catch (RejectedExecutionException e) {
//										waitForThreadPoolNotFullCondition();									
//									}
//								}
//							}
						}
//						writtenBlocks.add(ptr);
					} finally {
						cacheMap.returnBucket(ptr, true);
						CacheEntryPinner.getInstance().unpin(ptr, pinnedBlocks);
//						pinnedBlocks.clear();
					}
					
					Set<Key> writtenBlocks = new HashSet<Key>();
					int size = WonderDBPropertyManager.getInstance().getDiskAsyncWriterThreadPoolSize();
					if (bufferMap.size() >= size || isShutdown) {
						Iterator<Key> iter1 = bufferMap.keySet().iterator();
						List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
						while (iter1.hasNext()) {
							Key key = iter1.next();
							Data data = bufferMap.get(key);
							boolean flag = cacheMap.writtenBlocks.replace(key, 1, 2);
							if (flag) {
								while (true) {
									try {
										Future<Integer> future = writer.write(key, data);
										futures.add(future);
										break;
									} catch (Throwable e) {
										for (int i = 0; i < futures.size(); i++) {
											futures.get(i).get();
										}
	//									Thread.sleep(100);									
									}
								}
								writtenBlocks.add(key);
							}
						}

						for (int i = 0; i < futures.size(); i++) {
							futures.get(i).get();
						}
						
						iter1 = writtenBlocks.iterator();
						while (iter1.hasNext()) {
							Key ptr1 = iter1.next();
							boolean flag = cacheMap.writtenBlocks.remove(ptr1, 2);
							if (flag) {
								bufferMap.remove(ptr1);
							}
						}
//						bufferMap.clear();
//						writtenBlocks.clear();
					}								

				}
				timeTaken = System.currentTimeMillis()-start;


				syncTime = Math.max(syncTime, inprocessSyncTime);
				LogManager.getInstance().resetLogs(syncTime);
				if (isShutdown) {
					System.out.println("didnt write: "+cacheMap.writtenBlocks.size());
					if (cacheMap.writtenBlocks.size()>0 && shutc++ <= 5) {
						continue; 
					} else {
						shutdownOver = true;
						synchronized (shutdownLock) {
							shutdownLock.notifyAll();
						}
						return;
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public ByteBuffer copy(ChannelBuffer data) {
		data.clear();
		data.writerIndex(data.capacity());
		ChannelBuffer b = ChannelBuffers.copiedBuffer(data);
		b.clear();
		b.writerIndex(b.capacity());
		return b.toByteBuffer();
	}

//	
//	private void waitForThreadPoolNotFullCondition() {
//		try {
//			threadPoolFullLock.lock();
//			while (true) {
//				try {
//					threadPoolFullCondition.await(20, TimeUnit.MILLISECONDS);
//					break;
//				} catch (InterruptedException e) {
//				}
//			}
//		} finally {
//			threadPoolFullLock.unlock();
//		}
//	}
//	
//	private void notifyThreadPoolNotFull() {
//		try {
//			threadPoolFullLock.lock();
//			threadPoolFullCondition.signalAll();
//		} finally {
//			threadPoolFullLock.unlock();
//		}		
//	}
}
