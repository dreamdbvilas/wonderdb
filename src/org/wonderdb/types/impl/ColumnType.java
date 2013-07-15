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



public class ColumnType {
	final Object id;
	
	public ColumnType(Object id) {
		this.id = id;
	}
	
	public Object getValue() {
		return id;
	}
	
	public int getByteSize() {
		int size = 1 + // byte to indicate if its int or string
			Integer.SIZE/8; // int count if string else its int value ;
		if (id instanceof String) {
			size = size + id.toString().getBytes().length;
		}
		return size;
	}
	
	public int hashCode() {
		return id.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		
		if (o instanceof ColumnType) {
			return this.id.equals(((ColumnType) o).getValue());
		}
		
		if (id instanceof String && o instanceof String) {
			return id.equals(o);
		}
		
		if (id instanceof Integer && o instanceof Integer) {
			return id.equals(o);
		}
		return false;
	}
	
	public String getStringValue() {
		return id.toString();
	}
}
