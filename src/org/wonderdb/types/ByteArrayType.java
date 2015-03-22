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
package org.wonderdb.types;

import org.wonderdb.serialize.DefaultSerializer;

public class ByteArrayType implements DBType {
	byte[] bytes = null;
	
	public ByteArrayType(byte[] bytes) {
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
		
		return new ByteArrayType(byts);
		
	}
	
	public int getByteSize() {
		return DefaultSerializer.getInstance().getSize(this, null);
	}
	
	public int compareTo(DBType dt) {
		ByteArrayType bt = null;
		if (dt instanceof ByteArrayType) {
			bt = (ByteArrayType) dt;
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
		ByteArrayType bt = null;
		if (o instanceof ByteArrayType) {
			bt = (ByteArrayType) o;
		} else {
			return false;
		}
		
		return this.compareTo(bt) == 0; 
	}
}
