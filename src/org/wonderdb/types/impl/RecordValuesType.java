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

import java.util.ArrayList;
import java.util.List;

import org.wonderdb.types.SerializableType;


public class RecordValuesType {
	List<SerializableType> valuesByBlock = new ArrayList<SerializableType>();
	
	public RecordValuesType(List<SerializableType> list) {
		valuesByBlock = list;
	}
	
	public int getByteSize() {
		// we dont need to keep track of count or anything. header map did everything for us
		int size = 0;
		for (int i = 0; i < valuesByBlock.size(); i++) {
			SerializableType st = valuesByBlock.get(i);
			size = size + (st != null ? st.getByteSize() : 0);
		}
		return size;
	}
	
	public void setValues(List<SerializableType> list) {
		valuesByBlock = list;
	}
	
	public List<SerializableType> getValues() {
		return valuesByBlock;
	}

	public String getStringValue() {
		throw new RuntimeException("Method not supported");
	}
}
