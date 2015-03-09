package org.wonderdb.block.record.manager;

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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ObjectLocker {
	private static ObjectLocker instance = new ObjectLocker();
	ConcurrentMap<Object, Object> map = new ConcurrentHashMap<Object, Object>();
	
	private ObjectLocker() {	
	}
	
	public static ObjectLocker getInstance() {
		return instance;
	}
	
	public synchronized void acquireLock(Object id) {

		while (map.containsKey(id)) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
		map.put(id, id);
	}
	
	public synchronized void releaseLock(Object id) {
		map.remove(id);
		notifyAll();
	}
	
	public boolean queryInProcess(Object id) {
		return map.containsKey(id);
	}
}
