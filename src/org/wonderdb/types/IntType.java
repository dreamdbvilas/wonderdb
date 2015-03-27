package org.wonderdb.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


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

public class IntType implements DBType {
	Integer value = null;
		
	public IntType() {
	}
	
	public IntType(Integer v) {
		value = v;
	}
	
	public IntType copyOf() {
		return new IntType(value);
	}
	
	public void set(int v) {
		value = v;
	}
	
	public Integer get() {
		return value;
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
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof IntType) {
			return this.compareTo((IntType) o) == 0;
		}
		return false;
	}	
	
	@Override
	public int hashCode() {
		return value == null ? 0 : value.hashCode();
	}
}
