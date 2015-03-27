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

public class FloatType implements DBType {
	Float value = null;
	
	public FloatType() {
	}

	public FloatType(Float v) {
		value = v;
	}
	
	public Float get() {
		return value;
	}
	
	public FloatType copyOf() {
		return new FloatType(value);
	}
	
	public String getSerializerName() {
		return "fs";
	}
	
	public void set(String s) {
		try {
			value = Float.valueOf(s).floatValue();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
	
	public int compareTo(DBType t) {
		FloatType ft = null;
		if (t instanceof FloatType) {
			ft = (FloatType) t;
		} else {
			return -1;
		}

		if (this.value != null) {
			return this.value.compareTo(ft.value);
		} else if (ft.value != null) {
			return -1;
		}

		return 0;
	}

	public boolean equals(Object o) {
		if (o instanceof FloatType) {
			return this.compareTo((FloatType) o) == 0;
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
