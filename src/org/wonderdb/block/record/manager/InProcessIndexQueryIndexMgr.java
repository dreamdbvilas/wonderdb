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
package org.wonderdb.block.record.manager;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.types.impl.IndexKeyType;


public class InProcessIndexQueryIndexMgr {
	private static InProcessIndexQueryIndexMgr instance = new InProcessIndexQueryIndexMgr();
	
	ConcurrentMap<IndexKeyType, IndexKeyType> inprocessUniqueIndexKeyMap = new ConcurrentHashMap<IndexKeyType, IndexKeyType>();
	private InProcessIndexQueryIndexMgr() {
	}

	public static InProcessIndexQueryIndexMgr getInstance() {
		return instance;
	}
	
	public boolean canProcess(Set<IndexKeyType> keys) {
		Set<IndexKeyType> addedKeys = new HashSet<IndexKeyType>();
		Iterator<IndexKeyType> iter = keys.iterator();
		boolean pass = true;
		
		while (iter.hasNext()) {
			IndexKeyType ikt = iter.next();
			IndexKeyType current = inprocessUniqueIndexKeyMap.putIfAbsent(ikt, ikt);
			if (current != null) {
				pass = false;
				break;
			}
			addedKeys.add(ikt);
		}
		if (!pass) {
			iter = addedKeys.iterator();
			while (iter.hasNext()) {
				IndexKeyType ikt = iter.next();
				inprocessUniqueIndexKeyMap.remove(ikt);
			}
		}
		return pass;
	}
	
	public boolean canProcess(IndexKeyType ikt) {
		Set<IndexKeyType> set = new HashSet<IndexKeyType>();
		set.add(ikt);
		return canProcess(set);
	}
	
	public void done(IndexKeyType ikt) {
		Set<IndexKeyType> set = new HashSet<IndexKeyType>();
		set.add(ikt);
		done(set);
	}
	
	public void done(Set<IndexKeyType> keys) {
		if (keys == null) {
			return;
		}
		Iterator<IndexKeyType> iter = keys.iterator();
		while (iter.hasNext()) {
			IndexKeyType ikt = iter.next();
			inprocessUniqueIndexKeyMap.remove(ikt);
		}
	}
}
