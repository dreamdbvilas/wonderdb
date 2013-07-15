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

public interface CacheHandler<CacheContent> {

	public long getBlockCount();

	public ExternalReference<BlockPtr, CacheContent> get(BlockPtr ptr);
	public ExternalReference<BlockPtr, CacheContent> addIfAbsent(ExternalReference<BlockPtr, CacheContent> block);
	public ExternalReference<BlockPtr, CacheContent> getFromCache(BlockPtr ptr);	
	public void changed(ExternalReference<BlockPtr, CacheContent> ref);
	public void changed(BlockPtr ptr);
	public void remove(BlockPtr ptr);
	public ExternalReference<BlockPtr, CacheContent> removeForEvict(BlockPtr ptr);
	public void clear();
	public void shutdown();
}