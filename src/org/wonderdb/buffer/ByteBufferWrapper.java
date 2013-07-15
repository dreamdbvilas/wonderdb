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
package org.wonderdb.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferWrapper {
	List<byte[]> byteArrayList = null;
	
	public ByteBufferWrapper(ByteBuffer buffer) {
		byteArrayList = split(buffer);
	}
	
	public ByteBuffer get() {
		ByteBuffer buffer = ByteBuffer.allocate(byteArrayList.size()*100);
		for (int i = 0; i < byteArrayList.size(); i++) {
			buffer.put(byteArrayList.get(i));
		}
		return buffer;
	}
	
	private List<byte[]> split(ByteBuffer buffer) {
		List<byte[]> array = new ArrayList<byte[]>();
		int curPosn = 0;
		buffer.flip();
		buffer.position(0);
		int size = buffer.remaining();
		while (size > curPosn) {
			int aSize = 100;
			if (size-curPosn < 100) {
				aSize = size-curPosn;
			}
			byte[] a = new byte[aSize];
			buffer.get(a, 0, aSize);
			array.add(a);
			curPosn = curPosn + aSize;
		}
		return array;
	}
}
