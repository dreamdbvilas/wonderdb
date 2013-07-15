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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;


public class CacheMap<CacheContent> {
	List<ExternalReference<BlockPtr, CacheContent>>[] array = null;
	int concurrencyLevel = -1;
	int listSize;
	ReentrantReadWriteLock[] lock = null; 
	ConcurrentMap<BlockPtr, Integer> writtenBlocks = new ConcurrentHashMap<BlockPtr, Integer>();
	boolean trackWrites = false;
	
	@SuppressWarnings("unchecked")
	public CacheMap(int listSize, int concurrencyLevel, boolean trackWrites) {
		this.listSize = listSize;
		array = new ArrayList[listSize];		
		this.concurrencyLevel = concurrencyLevel;
		this.trackWrites = trackWrites;
		
		for (int i = 0; i < listSize; i++) {
			array[i] = new ArrayList<ExternalReference<BlockPtr, CacheContent>>();
		}
		lock = new ReentrantReadWriteLock[this.concurrencyLevel];
		for (int i = 0; i < lock.length; i++) {
			lock[i] = new ReentrantReadWriteLock();
		}
	}
	
	public ExternalReference<BlockPtr, CacheContent> addIfAbsent(ExternalReference<BlockPtr, CacheContent> ref) {
		if (ref == null || ref.getPtr() == null) {
			return null;
		}
		
		int listPosn = ref.getPtr().hashCode();
		listPosn = Math.abs(listPosn);
		listPosn = listPosn %listSize;
		List<ExternalReference<BlockPtr, CacheContent>> list = array[listPosn];
		int posn = 0;
		boolean add = false;
		int lockPosn = listPosn % concurrencyLevel; 
		lock[lockPosn].writeLock().lock();
		try {
			if (list.size() == 0) {
				add = true;
			} else {
				posn = Collections.binarySearch(list, ref.getPtr());
				if (posn >= 0) {
					return list.get(posn);
				} else {
					posn = Math.abs(posn) -1;
					add = true;
				}
			}

			if (add) {
				ref.setLastAccessTime(System.currentTimeMillis());
				list.add(posn, ref);					
			}
		} finally {
			lock[lockPosn].writeLock().unlock();
		}
		return ref;		
	}
	
	public ExternalReference<BlockPtr, CacheContent> get(BlockPtr bp) {
		if (!CacheEntryPinner.getInstance().isPinned(bp)) {
			Logger.getLogger(getClass()).fatal("Tryng to get entry from cache when its not pinned");
		}
		int listPosn = Math.abs(bp.hashCode())%listSize;
		List<ExternalReference<BlockPtr, CacheContent>> list = array[listPosn];
		int lockPosn = listPosn % concurrencyLevel;
		lock[lockPosn].readLock().lock();
		ExternalReference<BlockPtr, CacheContent> ref = null;
		
		try {
			if (list.size() > 0) {
				int p = Collections.binarySearch(list, bp);
				if (p >= 0) {
					ref = list.get(p);
					ref.setLastAccessTime(System.currentTimeMillis());
				} 
			}
			if (ref != null) {
				return ref.buildReference();
			} 
		} finally {
			lock[lockPosn].readLock().unlock();
		}
		
		return ref;
	}
	
	ExternalReference<BlockPtr, CacheContent> evict(BlockPtr ptr) {
		int listPosn = Math.abs(ptr.hashCode())%listSize;
		List<ExternalReference<BlockPtr, CacheContent>> list = array[listPosn];
		ExternalReference<BlockPtr, CacheContent> ref = null;
		int p = -1;
		int lockPosn = listPosn % concurrencyLevel;
		lock[lockPosn].writeLock().lock();
		try {
			if (list.size() > 0) {
				p = Collections.binarySearch(list, ptr);
				if (p >= 0) {
					ref = list.remove(p);
				}
			}
		} finally {
			lock[lockPosn].writeLock().unlock();
		}
		return ref;
	}
	
	ExternalReference<BlockPtr, CacheContent> evict (List<ExternalReference<BlockPtr, CacheContent>> list, int posn) {
		
		return list.remove(posn);
	}	
	
	public List<ExternalReference<BlockPtr, CacheContent>> getBucket(int id, boolean writeLock) {
		int lockPosn = id % concurrencyLevel;
		if (writeLock) {
			lock[lockPosn].writeLock().lock();
		} else {
			lock[lockPosn].readLock().lock();
		}
		return array[id];
	}
	
	public List<ExternalReference<BlockPtr, CacheContent>> getBucket(BlockPtr ptr, boolean writeLock) {
		int bucketPosn = Math.abs(ptr.hashCode()) % listSize;
		return getBucket(bucketPosn, writeLock);
	}
	
	public void returnBucket(int id, boolean writeLock) {
		int lockPosn = id % concurrencyLevel;
		if (writeLock) {
			lock[lockPosn].writeLock().unlock();
		} else {
			lock[lockPosn].readLock().unlock();
		}		
	}
	
	public void returnBucket(BlockPtr ptr, boolean writeLock) {
		int bucketPosn = Math.abs(ptr.hashCode()) % listSize;
		returnBucket(bucketPosn, writeLock);
	}
	
	public void changed(BlockPtr ptr) {
		if (trackWrites) {
			writtenBlocks.put(ptr, 0);
			synchronized (writtenBlocks) {
				writtenBlocks.notifyAll();
			}
		}
	}
	
	public boolean isChanged(BlockPtr ptr) {
		if (trackWrites && writtenBlocks.containsKey(ptr)) {
			return true;
		}
		return false;
	}
		
	public void clear() {
		for (int i = 0; i < listSize; i++) {
			array[i].clear();
		}		
	}	
}
