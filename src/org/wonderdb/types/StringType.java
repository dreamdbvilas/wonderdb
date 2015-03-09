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

public class StringType implements DBType {
	
	String value = null;
	
	public StringType(String v) {
		value = v;
	}
	
	public StringType copyOf() {
		if (value == null) {
			return new StringType(null);
		}
		return new StringType(new String(value));
	}
	
	public String get() {
		return value;
	}
	
	public int compareTo(DBType t) {
		StringType st = null;
		if (t instanceof StringType) {
			st = (StringType) t; 
		} else {
			return -1;
		}
		String i = st.get();
		
		if (i == null && value == null) {
			return 0;
		} else if (i != null && value == null) {
			return -1;
		} else if (i == null && value != null) {
			return 1;
		}
		
		int myLen = value.length();
		int len = i.length();
		int c = 0;
		if (t instanceof StringLikeType) {
			if (myLen > len) {
				int x = i.compareTo(value.substring(0, len));
				return x*-1;
			} else if (myLen < len) {
				return value.compareTo(i.substring(0, myLen));
			}			
		} else {
			if (myLen > len) {
				c = i.compareTo(value.substring(0, len));
				if (c == 0) {
					return 1;
				} 
				return c*-1;
			} else if (myLen < len) {
				c = value.compareTo(i.substring(0, myLen));
				if (c == 0) {
					return -1;
				}
				return c;
			}
		}
		return value.compareTo(i);
	}

	public String toString() {
		return value;
	}
	
	public boolean equals(Object o) {
		if (o instanceof StringType) {
			return this.compareTo((StringType) o) == 0;
		}
		return false;
	}
	
	public static void main(String[] args) {
		StringType l = new StringLikeType("vilas");
		StringType r = new StringLikeType("");
		
		int i = l.compareTo(r);
		System.out.println(i);
		
		i = r.compareTo(l);
		System.out.println(i);
	}
}
