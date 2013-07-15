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

public class BlockByteBuffer implements BlockBuffer {
	ByteBuffer buffer = null;
	public BlockByteBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}
	
	public void put(byte b) {
		buffer.put(b);
	}

	public byte get() {
		return buffer.get();
	}

	public void putInt(int i) {
		buffer.putInt(i);
	}

	public int getInt() {
		return buffer.getInt();
	}

	public void putLong(long l) {
		buffer.putLong(l);
	}

	public long getLong() {
		return buffer.getLong();
	}

	public void putDouble(double d) {
		buffer.putDouble(d);
	}

	public double getDouble() {
		return buffer.getDouble();
	}

	public void putFloat(float f) {
		buffer.putFloat(f);
	}

	public float getFloat() {
		return buffer.getFloat();
	}

	public void put(byte[] bytes) {
		buffer.put(bytes);
	}

	public byte[] get(int size) {
		byte[] bytes = new byte[size];
		buffer.get(bytes);
		return bytes;
	}

	public int getByteSize() {
		return buffer.capacity();
	}

	public void flip() {
		buffer.flip();
	}

	public void clear() {
		buffer.clear();
	}
	
	@Override
	public void clearResource() {
	}
	
	@Override
	public Object getBuffer() {
		return buffer;
	}
	
	@Override
	public void put(byte b, long ts) {
		updateTimestamp(ts);
		put(b);
	}

	@Override
	public void putInt(int i, long ts) {
		updateTimestamp(ts);
		putInt(i);
	}

	@Override
	public void putLong(long l, long ts) {
		updateTimestamp(ts);
		putLong(l);
	}

	@Override
	public void putDouble(double d, long ts) {
		updateTimestamp(ts);
		putDouble(d);
	}

	@Override
	public void putFloat(float f, long ts) {
		updateTimestamp(ts);
		putFloat(f);
	}

	@Override
	public void put(byte[] bytes, long ts) {
		updateTimestamp(ts);
		put(bytes);
	}
	
	@Override
	public int capacity() {
		return buffer.capacity();
	}
	
	@Override
	public int size() {
		return buffer.limit();
	}

	@Override
	public int remaining() {
		return buffer.remaining();
	}
	
	@Override
	public void reset() {
		buffer.reset();
	}
	
	@Override
	public void position(BlockBufferPosition p) {
		if (p instanceof BlockByteBufferPosition) {
			buffer.position(((BlockByteBufferPosition) p).position);
		}
	}
	
	@Override
	public BlockBufferPosition position() {
		return new BlockByteBufferPosition(buffer.position());
	}
	
	private void updateTimestamp(long ts) {
		buffer.mark();
		buffer.position(0);
		buffer.putLong(ts);
		buffer.reset();		
	}
	
	private static class BlockByteBufferPosition implements BlockBufferPosition {
		private final int position;
		
		private BlockByteBufferPosition(int p) {
			position = p;
		}
	}
}
