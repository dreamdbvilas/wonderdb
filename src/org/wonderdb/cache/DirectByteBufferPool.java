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
package org.wonderdb.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class DirectByteBufferPool {
	ConcurrentMap<Integer, ConcurrentLinkedQueue<ByteBuffer>> map = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<ByteBuffer>>();
	
	public static DirectByteBufferPool instance = new DirectByteBufferPool();
	
	public static DirectByteBufferPool getInstance() {
		return instance;
	}
	
	public ByteBuffer get(int size) {
		ConcurrentLinkedQueue<ByteBuffer> list = map.get(size);
		if (list == null) {
			list = map.putIfAbsent(size, new ConcurrentLinkedQueue<ByteBuffer>());
		}
		ByteBuffer b = list.poll();
		if (b == null) {
			b = ByteBuffer.allocate(size);
		}
		return b;
	}
	
	public void returnBuffer(ByteBuffer buffer) {
		int size = buffer.capacity();
		map.get(size).add(buffer);
	}
}
