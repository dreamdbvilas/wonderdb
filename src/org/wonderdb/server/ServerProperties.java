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
package org.wonderdb.server;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("serial")
class ServerProperties extends Properties {
	private static ServerProperties instance = new ServerProperties();
	private long totalMemory = -1;
	@SuppressWarnings("unused")
	private long cacheMemoryPercent = -1;
	@SuppressWarnings("unused")
	private long objectCacheMemoryPercent = -1;
	@SuppressWarnings("unused")
	private long tempMemoryPercent = -1;
	
	private ServerProperties() {
	}
	
	private ServerProperties(String fileName) throws IOException {
		super();
		super.load(new FileReader(fileName));
	}
	
	static void createInstance(String fileName) throws IOException {
		if (instance == null) {
			synchronized(ServerProperties.class) {
				if (instance == null) {
					instance = new ServerProperties(fileName);					
				}
			}
		}
		
		synchronized (instance) {
			
		}
	}
	
	public static ServerProperties getInstance() {
		return instance;
	}
	
	public long getTotalMemory() {
		return totalMemory;
	}
}
