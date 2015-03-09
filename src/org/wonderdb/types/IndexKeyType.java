package org.wonderdb.types;
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

import java.util.ArrayList;
import java.util.List;



public class IndexKeyType implements DBType {
	List<DBType> value;
	RecordId recordPosn = null;
//	int size = -1;
	
	public IndexKeyType(List<DBType> value, RecordId recPosn) {
		this.value = value == null ? new ArrayList<>() : new ArrayList<DBType>(value);
		this.recordPosn = recPosn;
	}

	public IndexKeyType copyOf() {
		List<DBType> list = new ArrayList<DBType>(value.size());
		for (int i = 0; i < value.size(); i++) {
			DBType dt = value.get(i);
			DBType add = null;
			if (dt != null) {
				add = dt.copyOf();
			}
			list.add(add);
		}
		IndexKeyType retVal = new IndexKeyType(list, recordPosn);
//		retVal.size = this.size;
		return retVal;
	}
	
	public RecordId getRecordId() {
		return recordPosn;
	}
	
	public DBType getKey() {
		return this;
	}
		
	public List<DBType> getValue() {
		return value;
	}
	
	public int compareTo(DBType k) {
		IndexKeyType key = null;
		if (k instanceof IndexKeyType) {
			key = (IndexKeyType) k;
		}
		
		int val = 0;
		
		int thisSize = value.size();
		int size = key.value.size();
		
		if (thisSize != size) {
			if (thisSize > size) {
				return 1;
			} else { 
				return -1;
			}
		}
		
		for (int i = 0; i < thisSize; i++) {
			DBType v1 = this.value.get(i);
			DBType v2 = key.value.get(i);
			if (v1 != null && v2 != null) {
				val = v1.compareTo(v2);
			} else {
				if (v1 == null && v2 == null) {
					continue;
				}
				if (v2 != null) {
					return -1;
				} else {
					return 1;
				}
			}
			if (val != 0) {
				return val;
			}
		}
		if (recordPosn != null) {
			return recordPosn.compareTo(key.recordPosn);
		}
		return 0;
	}
	
	public boolean equals(Object o) {
		if (o instanceof IndexKeyType) {
			return this.compareTo((IndexKeyType) o) == 0;
		}
		return false;
	}

	public String toString() {
		return value.toString() + ":" + recordPosn.toString();
	}
}
