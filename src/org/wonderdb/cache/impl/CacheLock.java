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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CacheLock {
	
	final Lock cleanupLock = new ReentrantLock(); 
	final Condition cacheNotFull = cleanupLock.newCondition();
	final Condition startCleanupCondition = cleanupLock.newCondition();
	final Condition startEagerCleanupCondition = cleanupLock.newCondition();
	
	final Lock syncupLock = new ReentrantLock();
	final Condition startSyncupCondition = syncupLock.newCondition();
	
	public CacheLock() {
	}
	
	public void waitOnCacheNotFull() {
		try {
			cleanupLock.lock();
			cacheNotFull.awaitUninterruptibly();
		} finally {
			cleanupLock.unlock();
		}
	}
	
	public void notifyCacheNotFull() {
		try {
			cleanupLock.lock();
			cacheNotFull.signalAll();
		} finally {
			cleanupLock.unlock();
		}
	}
	
	public void waitOnStartSyncup() {
		try {
			syncupLock.lock();
			startSyncupCondition.awaitUninterruptibly();
		} finally {
			syncupLock.unlock();
		}
	}
	
	public void notifyStartSyncup() {
		try {
			syncupLock.lock();
			startSyncupCondition.signalAll();
		} finally {
			syncupLock.unlock();
		}
	}
	
	public void waitOnEagerCleanup() {
		
		try {
			cleanupLock.lock();
			startEagerCleanupCondition.awaitUninterruptibly();
		} finally {
			cleanupLock.unlock();
		}
	}
	
	public void notifyEagerCleanup() {
		try {
			cleanupLock.lock();
			startEagerCleanupCondition.signalAll();
		} finally {
			cleanupLock.unlock();
		}
	}
	
	public void notifyStartCleanup() {
		try {
			cleanupLock.lock();
			startCleanupCondition.signalAll();
		} finally {
			cleanupLock.unlock();
		}
	}
	
	public void waitOnStartCleanup() {
		try {
			cleanupLock.lock();
			startCleanupCondition.awaitUninterruptibly();
		} finally {
			cleanupLock.unlock();
		}
	}
}

