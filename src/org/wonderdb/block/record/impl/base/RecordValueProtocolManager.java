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
package org.wonderdb.block.record.impl.base;

import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.util.internal.ConcurrentHashMap;

public class RecordValueProtocolManager {
	private static RecordValueProtocolManager instance = new RecordValueProtocolManager();
	
	ConcurrentMap<String, RecordValueExtractor> map = new ConcurrentHashMap<String, RecordValueExtractor>();
	private RecordValueProtocolManager() {
		registerRecordValueExtractor("xml", XMLValueExtractor.getInstance());
		registerRecordValueExtractor("json", JSONValueExtractor.getInstance());
	}
	
	public static RecordValueProtocolManager getInstance() {
		return instance;
	}
	
	public RecordValueExtractor getRecordValueExtractor(String protocol) {
		return map.get(protocol);
	}
	
	public void registerRecordValueExtractor(String protocol, RecordValueExtractor extractor) {
		RecordValueExtractor val = map.putIfAbsent(protocol, extractor);
		if (val != null) {
			throw new RuntimeException("Record Value Extractor Protocol: " + protocol + " already registered");
		}
	}
}
