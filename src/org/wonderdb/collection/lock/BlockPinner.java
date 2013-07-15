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
package org.wonderdb.collection.lock;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.block.BlockPtr;


public class BlockPinner {
	private static ConcurrentMap<BlockPtr, ConcurrentMap<Integer, Integer>> pinnedBlockPtr = new ConcurrentHashMap<BlockPtr, ConcurrentMap<Integer,Integer>>();
	private static ConcurrentMap<Integer, ConcurrentMap<BlockPtr, BlockPtr>> pinnedBlocksByQueryId = new ConcurrentHashMap<Integer, ConcurrentMap<BlockPtr,BlockPtr>>();
	private static BlockPinner instance = new BlockPinner();
	
	private BlockPinner() {
	}
	
	public static BlockPinner getInstance() {
		return instance;
	}
	
	public void pin(int queryId, BlockPtr ptr) {
		ConcurrentMap<Integer, Integer> cMap = pinnedBlockPtr.get(ptr);
		if (cMap == null) {
			synchronized (pinnedBlockPtr) {
				cMap = pinnedBlockPtr.get(ptr);
				if (cMap == null) {
					cMap = new ConcurrentHashMap<Integer, Integer>();
					pinnedBlockPtr.put(ptr, cMap);
				}
			}
		}
		cMap.put(queryId, queryId);
		
		ConcurrentMap<BlockPtr, BlockPtr> map = pinnedBlocksByQueryId.get(queryId);
		if (map == null) {
			synchronized (pinnedBlocksByQueryId) {
				map = pinnedBlocksByQueryId.get(queryId);
				if (map == null) {
					map = new ConcurrentHashMap<BlockPtr, BlockPtr>();
					pinnedBlocksByQueryId.put(queryId, map);
				}
			}
		}
		map.put(ptr, ptr);
	}
	
	public boolean isPinned(BlockPtr ptr) {
		return pinnedBlockPtr.get(ptr) != null;
	}
	
	public void unpin(int queryId) {
		ConcurrentMap<BlockPtr, BlockPtr> map = pinnedBlocksByQueryId.remove(queryId);
		Iterator<BlockPtr> iter = map.keySet().iterator();
		while (iter.hasNext()) {
			ConcurrentMap<Integer, Integer> cMap = pinnedBlockPtr.get(iter.next());
			if (cMap != null) {
				cMap.remove(queryId);
				
			}
		}
	}
}
