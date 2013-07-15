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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GrowableByteBufferList extends ByteBufferList {
	int bufferSize = -1;
	int arrayCount = -1;
	int reservedBytesCount = -1;
	
	public GrowableByteBufferList(int bufferSize, int arrayCount, int reservedBytesCount) {
		super(null);
		this.bufferSize = bufferSize;
		this.arrayCount = arrayCount;
		this.reservedBytesCount = reservedBytesCount;
		addBuffers();
		bufferList = new ArrayList<ByteBuffer>();
	}
	
	public GrowableByteBufferList(int bufferSize, List<ByteBuffer> list) {
		super(list);
		this.bufferSize = bufferSize;
	}
	
	@Override
	protected void setPosn(int size) {
		if (bufferList.get(posn).remaining() < size) {
			posn++;
			if (bufferList.size() < posn) {
				bufferList.add(ByteBuffer.allocate(bufferSize));
			}
		}
	}
	
	private void addBuffers() {
		for (int i = 0; i < arrayCount; i++) {
			ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
			if (i == 0) {
				byte[] bytes = new byte[reservedBytesCount];
				buffer.put(bytes);
			}
			bufferList.add(buffer);
		}		
	}
}
