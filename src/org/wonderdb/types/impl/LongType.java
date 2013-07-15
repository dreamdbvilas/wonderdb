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

import org.wonderdb.seralizers.LongSerializer;
import org.wonderdb.types.DBType;

public class LongType implements DBType {
	Long value;
	
	public int getByteSize() {
		return LongSerializer.getInstance().getByteSize(this);
	}
	
	public LongType(long v) {
		value = v;
	}
	
	public LongType copyOf() {
		return new LongType(value);
	}
	
	public void set(long v) {
		value = v;
	}
	
	public long get() {
		return value;
	}
	
	public String getSerializerName() {
		return "ls";
	}
	
	public void set(String s) {
		try {
			value = Long.valueOf(s).longValue();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
	
	public int compareTo(DBType k) {
		LongType lt = null;
		if (k instanceof LongType) {
			lt = (LongType) k;
		} else {
			return -1;
		}
		
		if (this.value != null) {
			return this.value.compareTo(lt.value);
		} else if (lt.value != null) {
			return -1;
		}

		return 0;
	}

	public boolean equals(Object o) {
		if (o instanceof LongType) {
			return this.compareTo((LongType) o) == 0;
		}
		return false;
	}

	public String toString() {
		return String.valueOf(value);
	}
	
	public long getBlockPosn() {
		throw new RuntimeException("Method not supported");		
	}
	
	public int getPosn() {
		throw new RuntimeException("Method not supported");
	}
}
