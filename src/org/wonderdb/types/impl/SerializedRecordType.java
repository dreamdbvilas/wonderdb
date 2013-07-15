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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.types.SerializableType;


public class SerializedRecordType  {
	BlockPtr headerBlock = null;
	RecordHeaderType recordHeader = null;
	Map<RecordId, SerializableType> valuesByBlock = null;
	Map<BlockPtr, SerializableType> valuesByPtr = null;
	
	public int getByteSize() {
		throw new RuntimeException("Method not supported");
	}
	
	public void setRecordHeader(BlockPtr ptr, RecordHeaderType st) {
		recordHeader = st;
		headerBlock = ptr;
	}
	
	public void setValuesByBlock(Map<RecordId, SerializableType> values) {
		valuesByBlock = values;
		valuesByPtr = new HashMap<BlockPtr, SerializableType>(values.size());
		Iterator<RecordId> iter = values.keySet().iterator();
		while (iter.hasNext()) {
			RecordId recId = iter.next();
			valuesByPtr.put(recId.getPtr(), valuesByBlock.get(recId));
		}
	}
	
	public int getByteSize(BlockPtr ptr) {
		int size = 0;
		if (headerBlock.equals(ptr)) {
			size = size + recordHeader.getByteSize();
		}
		
		SerializableType st = valuesByPtr.get(ptr);
		if (st != null) {
			size = size + st.getByteSize();
		}
		return size;
	}
}
