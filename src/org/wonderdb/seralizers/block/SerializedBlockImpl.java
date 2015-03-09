package org.wonderdb.seralizers.block;
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


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.types.BlockPtr;


public class SerializedBlockImpl implements Cacheable<BlockPtr, ChannelBuffer> {

	public static final int HEADER_SIZE = 1 + Long.BYTES + Long.BYTES;
	
	BlockPtr ptr = null;
	long lastAccessTime = -1;
	private ChannelBuffer fullBuffer = null;
	
	public SerializedBlockImpl(BlockPtr ptr, ChannelBuffer... buffers) {
		this.ptr = ptr;
		if (buffers == null) {
			return;
		}
		for (int i = 0; i < buffers.length; i++) {
			buffers[i].clear();
			buffers[i].writerIndex(buffers[i].capacity());
		}
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buffers);
		buffer.clear();
		fullBuffer = buffer;
	}
	
	public ChannelBuffer getData() {
		fullBuffer.clear();
		fullBuffer.writerIndex(fullBuffer.capacity());
		return fullBuffer.slice(HEADER_SIZE, fullBuffer.capacity()-HEADER_SIZE);
	}
	
	public int hashCode() {
		return ptr.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof SerializedBlockImpl) {
			return ptr.equals(o);
		}
		return false;
	}
	
	public ChannelBuffer getFullBuffer() {
		return fullBuffer;
	}
	
	@Override
	public int compareTo(BlockPtr ptr) {
		if (ptr == null) {
			return 1;
		}
		return this.ptr.compareTo(ptr);
	}

	@Override
	public void setLastAccessTime(long time) {
		this.lastAccessTime = time;
	}

	@Override
	public long getLastAccessTime() {
		return lastAccessTime;
	}

	@Override
	public BlockPtr getPtr() {
		return ptr;
	}

	@Override
	public ChannelBuffer getFull() {
		return fullBuffer;
	}	
}
