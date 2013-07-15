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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.DefaultClusterManager;
import org.wonderdb.thread.ThreadPoolExecutorWrapper;

public class ArchiveLogManager {
	private static ArchiveLogManager instance = new ArchiveLogManager();
	
	ConcurrentMap<Integer, AtomicLong> collectionObjectOrder = new ConcurrentHashMap<Integer, AtomicLong>();
	ConcurrentMap<Integer, ConcurrentLinkedQueue<ChannelBuffer>> collectionLogQueue = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<ChannelBuffer>>();
	
	Lock threadPoolFullLock = new ReentrantLock();
	Condition threadPoolFullCondition = threadPoolFullLock.newCondition();
	ThreadPoolExecutorWrapper executor = new ThreadPoolExecutorWrapper(10, 50, 5, 1000);
	
	private ArchiveLogManager() {
	}
	
	public static ArchiveLogManager getInstance() {
		return instance;
	}
	
	public void write(int schemaId, ChannelBuffer buffer, int writeCount) {
		
		AtomicLong al = collectionObjectOrder.get(schemaId);
		if (al == null) {
			al = new AtomicLong(0);
			AtomicLong tal = collectionObjectOrder.putIfAbsent(schemaId, al);
			if (tal != null) {
				al = tal;
			}
		}
		
		long id = al.getAndIncrement();
		ConcurrentLinkedQueue<ChannelBuffer> queue = null;
		
		LocalArchiveLogger.getInstance().log(schemaId, id, buffer);
		
		synchronized (collectionLogQueue) {
			queue = collectionLogQueue.get(schemaId);
			if (queue == null) {
				queue = new ConcurrentLinkedQueue<ChannelBuffer>();
				ConcurrentLinkedQueue<ChannelBuffer> q = collectionLogQueue.putIfAbsent(schemaId, queue);
				if (q != null) {
					queue = q;
				}
			}			
		}

		synchronized (queue) {
			queue.add(buffer);			
			while (queue.peek() != buffer) {
				try {
					queue.wait();
				} catch (InterruptedException e) {
				}
			}
		}
		
		writeToMedia(id, schemaId, buffer, writeCount);
		
		synchronized (collectionLogQueue) {
			queue.remove();
			if (queue.size() == 0) {
				collectionLogQueue.remove(queue);
			}
		}
		
		synchronized (queue) {
			queue.notifyAll();
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
	
	private int writeToMedia(long id, int schemaId, ChannelBuffer buffer, int writeCount) {
		Channel[] channels = null;
//		Channel[] channels = ClusterManagerFactory.getInstance().getClusterManager().getArchiveLogChannels(schemaId);
		@SuppressWarnings("unchecked")
		Future<Boolean>[] futures = new Future[channels.length];
		
		for (int i = 0; i < channels.length; i++) {
			ChannelBuffer buf = ChannelBuffers.buffer(buffer.capacity() + Long.SIZE/8 + Integer.SIZE/8);
			buf.writeInt(buf.capacity());
			buf.writeLong(id);
			buf.writeBytes(buffer);
			RemoteLogWriterTask task = new RemoteLogWriterTask(channels[i], buf);
			while (true) {
				try {
					executor.asynchrounousExecute(task);
					break;
				} catch (RejectedExecutionException e) {
					waitForThreadPoolNotFullCondition();
				}
			}
		}
		
		int count = 0;
		for (int i = 0; i < futures.length;) {
			boolean val = false;
			
			try {
				val = futures[i].get(1000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e1) {
				break;
			} catch (ExecutionException e1) {
				break;
			} catch (TimeoutException e1) {
				break;
			}
			
			if (val) {
				count++;
				if (count >= writeCount) {
					break;
				}
			}
			
			if (futures[i].isDone()) {
				break;
			}
			
			i = i++;
			i = i % futures.length;
		}
		
		return count;
	}
	
	private class RemoteLogWriterTask implements Callable<Boolean> {
		ChannelBuffer buffer = null;
		Channel channel = null;
		
		RemoteLogWriterTask(Channel channel, ChannelBuffer buffer) {
			this.buffer = buffer;
			this.channel = channel;
		}

		@Override
		public Boolean call() throws Exception {
			ChannelFuture future = channel.write(buffer);
			boolean retVal = false;
			
			try {
				future.await(30000);
			} catch (InterruptedException e) {
			}
			
			if (future.isDone()) {
				retVal = true;
			}

			notifyThreadPoolNotFull();
			return retVal;
		}
	}
}
