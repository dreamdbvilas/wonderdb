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

public class BlockBufferUtils {
	private static BlockBufferUtils instance = new BlockBufferUtils();
	private BlockBufferUtils() {
	}
	
	static public BlockBufferUtils getInstance() {
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	public List<ByteBuffer> getByteBufferList(BlockBuffer buffer) {
		List<ByteBuffer> list = new ArrayList<ByteBuffer>();
		if (buffer instanceof BlockByteBuffer) {
			list.add((ByteBuffer) buffer.getBuffer());
		} else if (buffer instanceof ByteBufferList) {
			list.addAll((List<ByteBuffer>) buffer.getBuffer());
		}
		return list;
	}
}
