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
package org.wonderdb.cache.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.wonderdb.cache.Cache;
import org.wonderdb.cache.Cacheable;


public class MemoryCacheMap<Key, Data> implements Cache<Key, Data> {
	List<Cacheable<Key, Data>>[] array = null;
	int concurrencyLevel = -1;
	int listSize;
	ReentrantReadWriteLock[] lock = null; 
	ConcurrentMap<Key, Integer> writtenBlocks = new ConcurrentHashMap<Key, Integer>();
	boolean trackWrites = false;
	
	@SuppressWarnings("unchecked")
	public MemoryCacheMap(int listSize, int concurrencyLevel, boolean trackWrites) {
		this.listSize = listSize;
		array = new ArrayList[listSize];		
		this.concurrencyLevel = concurrencyLevel;
		this.trackWrites = trackWrites;
		
		for (int i = 0; i < listSize; i++) {
			array[i] = new ArrayList<>();
		}
		lock = new ReentrantReadWriteLock[this.concurrencyLevel];
		for (int i = 0; i < lock.length; i++) {
			lock[i] = new ReentrantReadWriteLock();
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cache.Cache#addIfAbsent(org.wonderdb.cache.Cacheable)
	 */
	@Override
	public Cacheable<Key, Data> addIfAbsent(Cacheable<Key, Data> ref) {
		if (ref == null) {
			return null;
		}
		
		int listPosn = ref.hashCode();
		listPosn = Math.abs(listPosn);
		listPosn = listPosn %listSize;
		List<Cacheable<Key, Data>> list = array[listPosn];
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
	
	public void forceAdd(Cacheable<Key, Data> ref) {
		if (ref == null) {
			return;
		}
		
		int listPosn = ref.hashCode();
		listPosn = Math.abs(listPosn);
		listPosn = listPosn %listSize;
		List<Cacheable<Key, Data>> list = array[listPosn];
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
					ref.setLastAccessTime(System.currentTimeMillis());
					list.set(posn, ref);
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
	}

	
	/* (non-Javadoc)
	 * @see org.wonderdb.cache.Cache#get(Key)
	 */
	@Override
	public Cacheable<Key, Data> get(Key bp) {
		if (!CacheEntryPinner.getInstance().isPinned(bp)) {
			Logger.getLogger(getClass()).fatal("Tryng to get entry from cache when its not pinned");
		}
		int listPosn = Math.abs(bp.hashCode())%listSize;
		List<Cacheable<Key, Data>> list = array[listPosn];
		int lockPosn = listPosn % concurrencyLevel;
		lock[lockPosn].readLock().lock();
		Cacheable<Key, Data> ref = null;
		
		try {
			if (list.size() > 0) {
				int p = Collections.binarySearch(list, bp);
				if (p >= 0) {
					ref = list.get(p);
					ref.setLastAccessTime(System.currentTimeMillis());
				} 
			}
			if (ref != null) {
//				return ref.buildReference();
			} 
		} finally {
			lock[lockPosn].readLock().unlock();
		}
		
		return ref;
	}
	
	public Cacheable<Key, Data> evict(Key ptr) {
		int listPosn = Math.abs(ptr.hashCode())%listSize;
		List<Cacheable<Key, Data>> list = array[listPosn];
		Cacheable<Key, Data> ref = null;
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
	
	public Cacheable<Key, Data> evict (List<Cacheable<Key, Data>> list, int posn) {
		
		return list.remove(posn);
	}	
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cache.Cache#getBucket(int, boolean)
	 */
	public List<Cacheable<Key, Data>> getBucket(int id, boolean writeLock) {
		int lockPosn = id % concurrencyLevel;
		if (writeLock) {
			lock[lockPosn].writeLock().lock();
		} else {
			lock[lockPosn].readLock().lock();
		}
		return array[id];
	}
	
	public List<Cacheable<Key, Data>> getBucket(Key ptr, boolean writeLock) {
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
	
	public void returnBucket(Key ptr, boolean writeLock) {
		int bucketPosn = Math.abs(ptr.hashCode()) % listSize;
		returnBucket(bucketPosn, writeLock);
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cache.Cache#changed(Key)
	 */
	public void changed(Key ptr) {
		if (trackWrites) {
			writtenBlocks.put(ptr, 0);
			synchronized (writtenBlocks) {
				writtenBlocks.notifyAll();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cache.Cache#isChanged(Key)
	 */
	public boolean isChanged(Key ptr) {
		if (trackWrites && writtenBlocks.containsKey(ptr)) {
			return true;
		}
		return false;
	}
		
	/* (non-Javadoc)
	 * @see org.wonderdb.cache.Cache#clear()
	 */
	@Override
	public void clear() {
		for (int i = 0; i < listSize; i++) {
			array[i].clear();
		}		
	}	
}
