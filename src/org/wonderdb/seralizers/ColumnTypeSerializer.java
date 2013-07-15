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
package org.wonderdb.seralizers;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.impl.ColumnType;


public class ColumnTypeSerializer {
	// singleton
	private static ColumnTypeSerializer instance = new ColumnTypeSerializer();
	
	// singleton
	private ColumnTypeSerializer() {
	}
	
	// singleton
	public static ColumnTypeSerializer getInstance() {
		return instance;
	}
	
	public void toByte(ColumnType ct, ChannelBuffer buffer) {
		if (ct.getValue() instanceof Integer) {
			buffer.writeByte((byte) 0);
			buffer.writeInt((Integer) ct.getValue());
		} else {
			buffer.writeByte((byte) 1);
			buffer.writeInt(ct.getValue().toString().getBytes().length);
			buffer.writeBytes(ct.getValue().toString().getBytes());
		}
	}
	
	public ColumnType unmarshal(ChannelBuffer buffer) {
		byte b = buffer.readByte();
		Object o = null;
		if (b == 0) {
			o = buffer.readInt();
		} else {
			int len = buffer.readInt();
			byte[] bytes = new byte[len];
			buffer.readBytes(bytes);
			o = new String(bytes);
		}
		return new ColumnType(o);
	}
}
