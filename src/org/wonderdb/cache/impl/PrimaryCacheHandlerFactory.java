package org.wonderdb.cache.impl;

import java.util.List;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.record.Record;

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


public class PrimaryCacheHandlerFactory  {
	private static PrimaryCacheHandlerFactory instance = new PrimaryCacheHandlerFactory();
	private CacheHandler<BlockPtr, List<Record>> cacheHandler = null;
	public static PrimaryCacheHandlerFactory getInstance() {
		return instance;
	}
	
	public void setCacheHandler(CacheHandler<BlockPtr, List<Record>> cacheHandler) {
		this.cacheHandler = cacheHandler;
	}
	
	public CacheHandler<BlockPtr, List<Record>> getCacheHandler() {
		return cacheHandler;
	}
}
