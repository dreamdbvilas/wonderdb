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

public class LongType implements DBType {
	Long value = null;
	
	public LongType() {
	}
	
	public LongType(Long v) {
		value = v;
	}
	
	public LongType copyOf() {
		return new LongType(value);
	}
	
	public void set(long v) {
		value = v;
	}
	
	public Long get() {
		return value;
	}

	public void set(String s) {
		try {
			value = Long.valueOf(s).longValue();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
	
	@Override
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

	@Override
	public boolean equals(Object o) {
		if (o instanceof LongType) {
			return this.compareTo((LongType) o) == 0;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return value == null ? 0 : value.hashCode();
	}

	public String toString() {
		return String.valueOf(value);
	}
}
