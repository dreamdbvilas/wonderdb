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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.wonderdb.types.BlockPtr;



public class CacheEntryPinner {
	private static CacheEntryPinner instance = new CacheEntryPinner();
	private ConcurrentLinkedQueue<Object> list = new ConcurrentLinkedQueue<Object>();
	
	private CacheEntryPinner() {
	}
	
	public static CacheEntryPinner getInstance() {
		return instance;
	}
	
	public synchronized boolean isPinned(Object ptr) {
		return list.contains(ptr);
	}
	
	public void pin(Object ptr, Set<Object> pinnedBlocks) {
		if (ptr instanceof BlockPtr) {
			
		}
		if (pinnedBlocks != null && !pinnedBlocks.contains(ptr) && ptr != null) {
			list.add(ptr);
			pinnedBlocks.add(ptr);
		}
	}
	
	public void unpin(Object ptr, Set<Object> pinnedBlocks) {
		if (ptr instanceof Set<?>) {
			unpin((Set<Object>) ptr, pinnedBlocks);
		}
		if (pinnedBlocks.contains(ptr)) {
			list.remove(ptr);
			pinnedBlocks.remove(ptr);
		}
	}
	
	public boolean isAnyBlockPinned() {
		return list.peek() != null; 
	}
	
	public void unpin(Set<Object> set, Set<Object> pinnedBlocks) {
		Set<Object> s = null;
		if (set == pinnedBlocks) {
			s = new HashSet<Object>(pinnedBlocks);
		} else {
			s = set;
		}
		Iterator<Object> iter = s.iterator();
		while (iter.hasNext()) {
			unpin(iter.next(), pinnedBlocks);
		}
		set.clear();
	}
	
	public String toString() {
		return list.toString();
	}
}
