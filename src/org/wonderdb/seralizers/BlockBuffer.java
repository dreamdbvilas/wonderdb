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



public interface BlockBuffer {

	public abstract void put(byte b);

	public abstract byte get();

	public abstract void putInt(int i);

	public abstract int getInt();

	public abstract void putLong(long l);

	public abstract long getLong();

	public abstract void putDouble(double d);

	public abstract double getDouble();

	public abstract void putFloat(float f);
	
	public abstract float getFloat();

	public abstract void put(byte[] bytes);

	public abstract byte[] get(int size);

	public abstract void put(byte b, long ts);

	public abstract void putInt(int i, long ts);

	public abstract void putLong(long l, long ts);

	public abstract void putDouble(double d, long ts);

	public abstract void putFloat(float f, long ts);

	public abstract void put(byte[] bytes, long ts);

	public abstract int getByteSize();

	public abstract void flip();

	public abstract void clear();
	
	public int capacity();
	
	public void clearResource();
	
	public void reset();
	
	Object getBuffer();
	
	int size();
	
	public int remaining();
	
	public void position(BlockBufferPosition pos);
	
	public BlockBufferPosition position();
}
