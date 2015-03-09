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

public class CacheBean {
	private long maxSize = 0;
	private long startSyncSize = 0;
	private long cleanupHighWaterMark = 0;
	private long cleanupLowWaterMark = 0;
	
	public long getMaxSize() {
		return maxSize;
	}
	
	public void setMaxSize(long maxSize) {
		this.maxSize = maxSize;
	}
	
	public long getStartSyncupSize() {
		return startSyncSize;
	}
	
	public void setStartSyncSize(long startSyncSize) {
		this.startSyncSize = startSyncSize;
	}
	
	public long getCleanupHighWaterMark() {
		return cleanupHighWaterMark;
	}
	
	public void setCleanupHighWaterMark(long cleanupHighWaterMark) {
		this.cleanupHighWaterMark = cleanupHighWaterMark;
	}
	
	public long getCleanupLowWaterMark() {
		return cleanupLowWaterMark;
	}
	
	public void setCleanupLowWaterMark(long cleanupLowWaterMark) {
		this.cleanupLowWaterMark = cleanupLowWaterMark;
	}
}
