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
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;


public interface SerializedBlock extends ExternalReference<BlockPtr, ChannelBuffer> {
	public static final byte LEAF_BLOCK = 1;
	public static final byte BRANCH_BLOCK = 2;
	public static final byte DATA_LEAF_BLOCK = 3;
	public static final byte INDEX_LEAF_BLOCK = 4;
	
	
	byte getBlockType();
	int getReserved();
	void setReserved(int val);
	
	public TransactionLogSerializedBlock getTransactionLogBLock();
	
	ChannelBuffer getDataBuffer();
	ChannelBuffer getFullBuffer();
}
