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

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;

public class CacheEntry implements Comparable<BlockPtr> {
	private long lastAccessed = -1;
	private long lastModified = -1;
	private ExternalReference<BlockPtr, ? extends Object> externalObj = null;
	
	public CacheEntry(ExternalReference<BlockPtr, ? extends Object> obj) {
		this.externalObj = obj;
	}
	
	public byte getFileId() {
		return externalObj.getPtr().getFileId();
	}
	
	public BlockPtr getBlockPtr() {
		return externalObj.getPtr();
	}
	
	public long getLastAccessed() {
		return lastAccessed;
	}
	
	public long getLastModified() {
		return lastModified;
	}
	
	public void setLastAccessed(long d) {
		lastAccessed = d;
	}
	
	public void setLastModified(long d) {
		lastModified = d;
	}
	
	public ExternalReference<BlockPtr, ? extends Object> getExternalReference() {
		return externalObj;
	}

	public int compareTo(BlockPtr p) {
		return externalObj.getPtr().compareTo(p);
	}
}