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
package org.wonderdb.block.record.impl.base;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.ExternalReference;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.BlockBuffer;
import org.wonderdb.seralizers.BlockBufferPosition;
import org.wonderdb.seralizers.BlockByteBuffer;
import org.wonderdb.seralizers.ByteBufferList;


public class RecordUtilities {
	private static RecordUtilities instance = new RecordUtilities();
	
	private RecordUtilities() {
	}
	
	public static RecordUtilities getInstance() {
		return instance;
	}
	
	public BlockBuffer getPosnData(ExternalReference<BlockPtr, ? extends BlockBuffer> recordBlock, int posn) {
		
		BlockBuffer buffer = recordBlock.getData();
		BlockBuffer b = createBuffer(buffer);
		buffer.reset();
		int count = buffer.getInt();
		
		for (int i = 0; i < count; i++) {
			int p = buffer.getInt();
			if (p == posn) {
				b.put(buffer.get(p));
				int len = buffer.getInt();
				byte[] bytes = buffer.get(len);
				b.put(bytes);
				break;
			}
		}
		buffer.reset();
		return b;
	}
	
	public void moveToLast(ExternalReference<BlockPtr, ? extends BlockBuffer> recordBlock, int posn) {
		BlockBuffer buffer = recordBlock.getData();
		BlockBuffer b = createBuffer(buffer);
		
		buffer.reset();
		int count = buffer.getInt();
		BlockBufferPosition origPosition = buffer.position();
		BlockBufferPosition copyToPosn = null;
		BlockBufferPosition copyFromPosn = null;
		
		for (int i = 0; i < count; i++) {
			int p = buffer.getInt();
			if (p == posn) {
				copyToPosn = buffer.position();
				b.put(buffer.get(p));
				continue;
			}
			byte[] bytes = buffer.get(p);
			if (copyToPosn != null) {
				copyFromPosn = buffer.position();
				buffer.reset();
				buffer.position(copyFromPosn);
				buffer.putInt(p);
				buffer.put(bytes);
				copyToPosn = buffer.position();
				buffer.position(copyFromPosn);
			}
		}
		buffer.position(origPosition);
	}
	
	@SuppressWarnings("unchecked")
	public BlockBuffer merge(List<BlockBuffer> bufList) {
		List<ByteBuffer> list = new ArrayList<ByteBuffer>();
		ByteBufferList buffer = new ByteBufferList(list);
		
		for (int i = 0; i < bufList.size(); i++) {
			BlockBuffer b = bufList.get(i);
			if (b instanceof BlockByteBuffer) {
				ByteBuffer bb = (ByteBuffer) b.getBuffer();
				list.add(bb);
			} else if (b instanceof ByteBufferList) {
				List<ByteBuffer> bbl = (List<ByteBuffer>) b.getBuffer();
				list.addAll(bbl);
			}
		}
		
		return buffer;
	}
	
	public List<BlockBuffer> readRecordSlot(ExternalReference<BlockPtr, ? extends BlockBuffer> ref, int posn) {
		List<BlockBuffer> retList = new ArrayList<BlockBuffer>();
		BlockBuffer buffer = RecordUtilities.getInstance().getPosnData(ref, posn);
		buffer.reset();
		while (buffer.remaining() > 0) {
			int size = buffer.getInt();
			BlockBuffer b = RecordUtilities.getInstance().createBuffer(size);
			byte[] bytes = buffer.get(size);
			b.put(bytes);
			retList.add(b);
		}
		
		return retList;
	}
	
	private BlockBuffer createBuffer(BlockBuffer buffer) {
		BlockBuffer retVal = null;
		if (buffer instanceof ByteBufferList) {
			List<ByteBuffer> list = ((ByteBufferList) buffer).getList();
			int capacity = list.get(0).capacity();
			List<ByteBuffer> tmpList = new ArrayList<ByteBuffer>(list.size());
			retVal = new ByteBufferList(tmpList);
			for (int i = 0; i < list.size(); i++) {
				tmpList.add(ByteBuffer.allocate(capacity));
			}
		} else if (buffer instanceof BlockByteBuffer) {
			ByteBuffer b = (ByteBuffer) ((BlockByteBuffer) buffer).getBuffer();
			retVal = new BlockByteBuffer(ByteBuffer.allocate(b.capacity()));
		}
		return retVal;
	}	
	
	private BlockBuffer createBuffer(int size) {
		BlockBuffer buffer = null;
		int s = StorageUtils.getInstance().getSmallestBlockSize();
		double d1 = (double) (size/s) + 0.5;
		BigDecimal d = new BigDecimal(d1) ;
		d = d.round(new MathContext(1, RoundingMode.UP));
		int i = d.intValue(); 
		if (i <= 1) {
			ByteBuffer b = ByteBuffer.allocate(s);
			buffer = new BlockByteBuffer(b);
		} else {
			List<ByteBuffer> list = new ArrayList<ByteBuffer>();
			for (int x = 0; x < i; x++) {
				list.add(ByteBuffer.allocate(s));
			}
			buffer = new ByteBufferList(list);
		}
		return buffer;
	}
}
