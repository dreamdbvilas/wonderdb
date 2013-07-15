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

import org.wonderdb.seralizers.ByteSerializer;
import org.wonderdb.types.DBType;

public class ByteType implements DBType {
	byte[] bytes = null;
	
	public ByteType(byte[] bytes) {
		this.bytes = bytes;
	}
	
	public String getSerializerName() {
		return "bt";
	}
	
	public byte[] get() {
		return bytes;
	}
	
	public DBType copyOf() {
		byte[] byts = new byte[bytes.length];
		System.arraycopy(bytes, 0, byts, 0, bytes.length);
		
		return new ByteType(byts);
		
	}
	
	public int getByteSize() {
		return ByteSerializer.getInstance().getByteSize(this);
	}
	
	public int compareTo(DBType dt) {
		ByteType bt = null;
		if (dt instanceof ByteType) {
			bt = (ByteType) dt;
		} else {
			return -1;
		}
		int thisLen = bytes!=null?bytes.length:0;
		int btLen = bt.bytes!=null?bt.bytes.length:0;
		
		if (btLen < thisLen) {
			return 1;
		}
		
		if (btLen > thisLen) {
			return -1;
		}
		
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] == bt.bytes[i]) {
				continue;
			}
			if (bytes[i] > bt.bytes[i]) {
				return 1;
			}
			if (bytes[i] < bt.bytes[i]) {
				return -1;
			}
		}
		
		return 0;
	}
	
	public boolean equals(Object o) {
		ByteType bt = null;
		if (o instanceof ByteType) {
			bt = (ByteType) o;
		} else {
			return false;
		}
		
		return this.compareTo(bt) == 0; 
	}
}
