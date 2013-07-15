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
package org.wonderdb.types.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.wonderdb.block.record.manager.RecordId;


public class RecordHeaderType {
	int recordPosn;
	Map<RecordId, List<ColumnType>> loadedMap = new HashMap<RecordId, List<ColumnType>>();
	
	public int getRecordPosn() {
		return recordPosn;
	}
	
	public void setRecordPosn(int p) {
		recordPosn = p;
	}
	
	
	public int getByteSize() {
		int size = 0;
		size = size + Integer.SIZE/8; /* map count */
		size = size + (loadedMap.size() * (1 + Long.SIZE/8 + Integer.SIZE/8)); /* space for record ids */
		size = size + Integer.SIZE/8;
		Iterator<List<ColumnType>> iter = loadedMap.values().iterator();
		while (iter.hasNext()) {
			Collection<ColumnType> list = iter.next();
			Iterator<ColumnType> ctIter = list.iterator();
			size = size + Integer.SIZE/8; /* list count */
			while (ctIter.hasNext()) {
				ColumnType ct = ctIter.next();
				size = size + ct.getByteSize();
			}
		}
		return size;
	}
	
	public Map<RecordId, List<ColumnType>> getHeader() {
		return loadedMap;
	}
	
	public void setHeaderMap(Map<RecordId, List<ColumnType>> map) {
		loadedMap = map;
	}	
	
	public void addDataChunk(RecordId dataRecordId, List<ColumnType> headerList) {
		loadedMap.put(dataRecordId, headerList);
	}
	
	public String getStringValue() {
		throw new RuntimeException("Method not supported");
	}
	
	public RecordId find(ColumnType ct) {		
		Iterator<RecordId> iter = loadedMap.keySet().iterator();
		Collection<ColumnType> columns = null;
		while (iter.hasNext()) {
			RecordId recordId = iter.next();
			columns = loadedMap.get(recordId);
			if (columns.contains(ct)) {
				return recordId;
			}
		}
		return null;
	}
	
	public void remove(ColumnType ct) {
		Iterator<List<ColumnType>> iter = loadedMap.values().iterator();
		Collection<ColumnType> columns = null;
		while (iter.hasNext()) {
			columns = iter.next();
			if (columns.remove(ct)) {
				break;
			}
		}		
	}
}
