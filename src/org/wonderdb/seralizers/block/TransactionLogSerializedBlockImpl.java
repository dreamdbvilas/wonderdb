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
package org.wonderdb.seralizers.block;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.schema.StorageUtils;


public class TransactionLogSerializedBlockImpl implements TransactionLogSerializedBlock {
	ChannelBuffer[] buffers = null;
	ChannelBuffer buffer = null;
	
	public TransactionLogSerializedBlockImpl(ChannelBuffer buffer) {
		int smallBufferSize = StorageUtils.getInstance().getSmallestBlockSize();
		int smallBufferCount = buffer.capacity()/smallBufferSize;
		buffers = new ChannelBuffer[smallBufferCount];
		this.buffer = buffer;
		for (int i = 0; i < smallBufferCount; i++) {
			int posn = i*smallBufferCount;
			buffers[i] = buffer.slice(posn, smallBufferSize);
		}
	}

	@Override
	public int getTransactionBlockCount() {
		ChannelBuffer buffer = buffers[0];
		buffer.writerIndex(buffer.capacity());
		buffer.readerIndex(SerializedBlockImpl.TXN_ID_POSN);
		return buffer.readInt();
	}
	
	@Override
	public void setTrnsactionBlockCount(int id) {
		for (int i = 0; i < buffers.length; i++) {
			ChannelBuffer b = buffers[0];
			b.clear();
			b.writerIndex(SerializedBlockImpl.TXN_ID_POSN);
			b.writeInt(id);
		}
	}

	@Override
	public ChannelBuffer getBuffer() {
		return buffer;
	}
}
