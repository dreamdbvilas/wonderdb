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

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.BlockPtr;


public class SecondaryCacheHandlerFactory {
	private static SecondaryCacheHandlerFactory instance = new SecondaryCacheHandlerFactory();
	CacheHandler<BlockPtr, ChannelBuffer> cacheHandler = null;
	
	public static SecondaryCacheHandlerFactory getInstance() {
		return instance;
	}
	
	public void setCacheHandler(CacheHandler<BlockPtr, ChannelBuffer> handler) {
		cacheHandler = handler;
	}
	
	public CacheHandler<BlockPtr, ChannelBuffer> getCacheHandler() {
		return cacheHandler;
	}
}
