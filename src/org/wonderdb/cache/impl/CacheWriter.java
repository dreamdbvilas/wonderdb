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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.wonderdb.cache.CacheDestinationWriter;
import org.wonderdb.cache.Cacheable;
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
		currentThread.interrupt();
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
	
	public void forceStartWriting() {
		if (writerFrequency > 0) {
			currentThread.interrupt();
		}
	}
	
	@Override
	public void run() {
		currentThread = Thread.currentThread();
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
				
				Iterator<Key> iter = cacheMap.writtenBlocks.keySet().iterator();
				Set<Object> pinnedBlocks = new HashSet<>();
				inprocessSyncTime = -1;
				Set<Key> writtenBlocks = new HashSet<>();
				while (iter.hasNext()) {
					Key ptr = iter.next();
					cacheMap.getBucket(ptr, true);
					try {
						
						if (!CacheEntryPinner.getInstance().isPinned(ptr) && cacheMap.writtenBlocks.get(ptr) != 1) {
							CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
							Cacheable<Key, Data> block = cacheMap.get(ptr);
							
							if (block == null) {
								continue;
							}
							boolean flag = cacheMap.writtenBlocks.replace(ptr, 0, 1);
							if (flag) {
								writer.copy(block.getFull());
								flag = cacheMap.writtenBlocks.replace(ptr, 1, 2);
							} else {
								continue;
							}
							if (flag) {
								while (true) {
									try {
										writer.write(block.getPtr());
										break;
									} catch (RejectedExecutionException e) {
										waitForThreadPoolNotFullCondition();									
									}
								}
							}
						}
						writtenBlocks.add(ptr);
					} finally {
						cacheMap.returnBucket(ptr, true);
						CacheEntryPinner.getInstance().unpin(ptr, pinnedBlocks);
					}								
				}
				
				syncTime = Math.max(syncTime, inprocessSyncTime);
				LogManager.getInstance().resetLogs(syncTime);
				if (isShutdown) {
					shutdownOver = true;
					synchronized (shutdownLock) {
						shutdownLock.notifyAll();
					}
					return;
				}
				
				Iterator<Key> iter1 = writtenBlocks.iterator();
				while (iter1.hasNext()) {
					Key ptr = iter1.next();
					boolean f = cacheMap.writtenBlocks.remove(ptr, 2);
					int i = 0;
				}
			} catch (Exception e) {
				e.printStackTrace();
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
}
