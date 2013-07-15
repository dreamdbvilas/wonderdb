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

import org.wonderdb.seralizers.IntSerializer;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.DBType;

public class IntType implements DBType, Cacheable {
	Integer value;
		
	public IntType() {
		value = -1;
	}
	
	public IntType(int v) {
		value = v;
	}
	
	public IntType copyOf() {
		return new IntType(value);
	}
	
	public int getByteSize() {
		return IntSerializer.getInstance().getByteSize(this);
	}
	
	public void set(int v) {
		value = v;
	}
	
	public int get() {
		return value;
	}
	
	public String getSerializerName() {
		return "is";
	}
	
	public String toString() {
		return String.valueOf(value);
	}
	
	public void set(String s) {
		try {
			value = Integer.valueOf(s).intValue();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
	
	public int compareTo(DBType t) {
		IntType it = null;
		if (t instanceof IntType) {
			it = (IntType) t;
		} else {
			return -1;
		}
		
		if (this.value != null) {
			return this.value.compareTo(it.value);
		} else if (it.value != null) {
			return -1;
		}

		return 0;
	}
	
	public boolean equals(Object o) {
		if (o instanceof IntType) {
			return this.compareTo((IntType) o) == 0;
		}
		return false;
	}
	
	public long getBlockPosn() {
		throw new RuntimeException("Method not supported");		
	}
	
	public int getPosn() {
		throw new RuntimeException("Method not supported");
	}
}
