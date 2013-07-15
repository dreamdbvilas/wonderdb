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
import java.util.List;

public class ByteBufferList implements BlockBuffer {
	protected List<ByteBuffer> bufferList = null;
	int posn = 0;
	
	public ByteBufferList(List<ByteBuffer> list) {
		this.bufferList = list;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#put(byte)
	 */
	@Override
	public void put(byte b) {
		setPosn(1);
		bufferList.get(posn).put(b);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#get()
	 */
	@Override
	public byte get() {
		setPosn(1);
		return bufferList.get(posn).get();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#putInt(int)
	 */
	@Override
	public void putInt(int i) {
		setPosn(Integer.SIZE/8);
		bufferList.get(posn).putInt(i);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#getInt()
	 */
	@Override
	public int getInt() {
		setPosn(Integer.SIZE/8);
		return bufferList.get(posn).getInt();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#putLong(long)
	 */
	@Override
	public void putLong(long l) {
		setPosn(Long.SIZE/8);
		bufferList.get(posn).putLong(l);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#getLong()
	 */
	@Override
	public long getLong() {
		setPosn(Long.SIZE/8);
		return bufferList.get(posn).getLong();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#putDouble(double)
	 */
	@Override
	public void putDouble(double d) {
		setPosn(Double.SIZE/8);
		bufferList.get(posn).putDouble(d);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#getDouble()
	 */
	@Override
	public double getDouble() {
		setPosn(Double.SIZE/8);
		return bufferList.get(posn).getDouble();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#putFloat(float)
	 */
	@Override
	public void putFloat(float f) {
		setPosn(Float.SIZE/8);
		bufferList.get(posn).putFloat(f);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#getFloat()
	 */
	@Override
	public float getFloat() {
		setPosn(Float.SIZE/8);
		return bufferList.get(posn).getFloat();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#put(byte[])
	 */
	@Override
	public void put(byte[] bytes) {
		put(bytes, -1);
	}

	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#get(int)
	 */
	@Override
	public byte[] get(int size) {
		int bPosn = 0;
		byte[] bytes = new byte[size];
		while (bufferList.get(posn).remaining() < (size-bPosn)) {
			int remaining = bufferList.get(posn).remaining();
			int length = Math.min(remaining, bytes.length-bPosn);
			bufferList.get(posn).get(bytes, bPosn, length);
			bPosn = bPosn + length;
			posn++;
		}
		bufferList.get(posn).get(bytes, bPosn, bytes.length-bPosn);
		return bytes;
	}

	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#getByteSize()
	 */
	@Override
	public int getByteSize() {
		return bufferList == null || bufferList.size() == 0 ? 0 : bufferList.size() * bufferList.get(0).capacity();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#clearList()
	 */
	@Override
	public void clearResource() {
		posn = 0;
		bufferList.clear();
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#removeLast()
	 */
	public ByteBuffer removeLast() {
		return bufferList.remove(bufferList.size()-1);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#add(java.nio.ByteBuffer)
	 */
	public void add(ByteBuffer buffer) {
		bufferList.add(buffer);
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#getList()
	 */
	public List<ByteBuffer> getList() {
		return bufferList;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#flip()
	 */
	@Override
	public void flip() {
		posn = 0;
		for (int i = 0; i < bufferList.size(); i++) {
			bufferList.get(i).flip();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.BlockBuffer#clear()
	 */
	@Override
	public void clear() {
		posn = 0;
		for (int i = 0; i < bufferList.size(); i++) {
			bufferList.get(i).clear();
		}
	}
	
	@Override
	public void reset() {
		posn = 0;
		for (int i = 0; i < bufferList.size(); i++) {
			bufferList.get(i).reset();
		}		
	}
	
	@Override
	public int size() {
		int size = 0;
		for (int i = 0; i <= posn; i++) {
			size = size + bufferList.get(i).limit();
		}
		return size;
	}
	
	@Override
	public Object getBuffer() {
		return bufferList;
	}
	
	@Override
	public int capacity() {
		if (bufferList == null || bufferList.size() == 0) {
			return 0;
		}
		return bufferList.size() * bufferList.get(0).capacity();
	}
	
	@Override
	public void put(byte b, long ts) {
		setPosn(1);
		updateTimestamp(ts);
		bufferList.get(posn).put(b);
	}

	@Override
	public void putInt(int i, long ts) {
		setPosn(Integer.SIZE/8);
		updateTimestamp(ts);
		bufferList.get(posn).putInt(i);
	}

	@Override
	public void putLong(long l, long ts) {
		setPosn(Long.SIZE/8);
		updateTimestamp(ts);
		bufferList.get(posn).putLong(l);
	}

	@Override
	public void putDouble(double d, long ts) {
		setPosn(Double.SIZE/8);
		updateTimestamp(ts);
		bufferList.get(posn).putDouble(d);
	}

	@Override
	public void putFloat(float f, long ts) {
		setPosn(Float.SIZE/8);
		updateTimestamp(ts);
		bufferList.get(posn).putFloat(f);
	}

	@Override
	public void put(byte[] bytes, long ts) {
		int bPosn = 0;
		while (bufferList.get(posn).remaining() < bytes.length-bPosn) {
			int remaining = bufferList.get(posn).remaining();
			int length = Math.min(remaining, bytes.length-bPosn);
			if (ts >= 0) {
				updateTimestamp(ts);
			}
			bufferList.get(posn).put(bytes, bPosn, length);
			bPosn = bPosn + length;
			posn++;
		}
		if (ts >= 0) {
			updateTimestamp(ts);
		}
		bufferList.get(posn).put(bytes, bPosn, bytes.length-bPosn);		
	}

	@Override
	public int remaining() {
		int remaining = 0;
		for (int i = posn; i < bufferList.size(); i++) {
			remaining = remaining + bufferList.get(i).remaining();
		}
		return remaining;
	}
	
	public void addAll(List<ByteBuffer> list) {
		bufferList.addAll(list);
	}
	
	@Override 
	public void position(BlockBufferPosition pos) {
		if (pos instanceof ByteBufferListPosition) {
			ByteBufferListPosition p = (ByteBufferListPosition) pos;
			this.posn = p.position;
			bufferList.get(posn).position(p.bufferPosition);
		}
	}
	
	@Override
	public BlockBufferPosition position() {
		return new ByteBufferListPosition(posn, bufferList.get(posn).position());
	}
	
	protected void setPosn(int size) {
		if (bufferList.get(posn).remaining() < size) {
			posn++;
		}
	}

	private void updateTimestamp(long ts) {
		ByteBuffer buffer = bufferList.get(posn);
		buffer.mark();
		buffer.position(0);
		buffer.putLong(ts);
		buffer.reset();		
	}
	
	private static class ByteBufferListPosition implements BlockBufferPosition {
		final int position;
		final int bufferPosition;
		
		private ByteBufferListPosition(int position, int bufferPosition) {
			this.position = position;
			this.bufferPosition = bufferPosition;
		}
	}
}
