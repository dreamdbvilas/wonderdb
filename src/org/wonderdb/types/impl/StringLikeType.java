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

import org.wonderdb.types.DBType;

public class StringLikeType extends StringType {

	public StringLikeType(String v) {
		super(v);
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
		if (myLen > len) {
			String s = value.substring(0, len);
			return s.compareTo(i);
		} else if (myLen < len) {
			return value.compareTo(i.substring(0, myLen));
		} 
		return value.compareTo(i);
	}
}
