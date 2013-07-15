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
package org.wonderdb.block.index.impl.base;

import org.wonderdb.block.index.IndexEntry;
import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.types.DBType;

public  class BaseIndexEntry implements IndexEntry {
	DBType key = null;
	QueriableBlockRecord record = null;
	
	public DBType getKey() {
		return key;
	}
	
	public void setKey(DBType key) {
		this.key = key;
	}
	
	public QueriableBlockRecord getRecord() {
		return record;
	}
	
	public void setValue(QueriableBlockRecord value) {
		record = value;
	}
	
//	public long getMemoryFootprint() {
//		if (record != null) {
//			return key.getMemoryFootprint() + record.getMemoryFootprint() + (Constants.POINTER_SIZE*2);
//		}
//		return key.getMemoryFootprint() + (Constants.POINTER_SIZE*2);
//	}
}
