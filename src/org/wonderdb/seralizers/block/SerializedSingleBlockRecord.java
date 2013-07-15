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

import java.util.List;

import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;


public class SerializedSingleBlockRecord implements SerializedRecordColumns {

	@Override
	public RecordId getRecordId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxContiguousFreeSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void update(List<ColumnType> columns, List<SerializableType> values) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SerializableType getValue(ColumnType columnType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ColumnType> getColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SerializableType> getValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getTotalFreeSize() {
		// TODO Auto-generated method stub
		return 0;
	}
//	SerializedRecordChunk recordChunk = null;
//	RecordId recId = null;
//	long reserved;
//	List<ColumnType> columns = null;
//	List<SerializableType> values = null;
//	int schemaId = -1;
//	SerializedRecordBlock srb = null;
//	
//	public SerializedSingleBlockRecord(RecordId recId, List<ColumnType> columns, CacheHandler<SerializedBlock> cacheHandler, int schemaId) {
//		this.recId = recId;
//		this.columns = columns;
//		loadRecord(recId, cacheHandler, schemaId);		
//	}
//	
//	public SerializedSingleBlockRecord(SerializedRecordBlock srb, RecordId recId, int size, long reserved, 
//			CacheHandler<SerializedBlock> cacheHandler, CacheResourceProvider<SerializedBlock> resourceProvider, byte recordType) {
//		this.recId = recId;
//		this.srb = srb;
//		createNewRecord(srb, recId.getPosn(), size, reserved, cacheHandler, resourceProvider, recordType);
//	}
//
//	private void loadRecord(RecordId recordId,
//			CacheHandler<SerializedBlock> cacheHandler, int schemaId) {
//		SerializedBlock sb = cacheHandler.getIgnorePin(recordId.getPtr()).getData();
//		srb = new SerializedRecordBlock(sb, false);
//		ChannelBuffer buffer = srb.get(recordId.getPosn());
//		RecordValuesType rvt = RecordValuesTypeSerializer.getInstance().unmarshal(buffer, columns, schemaId);
//		values = rvt.getValues();
//	}
//
//	private ChannelBuffer newSize(int size,
//			CacheHandler<SerializedBlock> cacheHandler,
//			CacheResourceProvider<SerializedBlock> resourceProvider) {
//		ChannelBuffer buffer = srb.get(recId.getPosn());
//		int currentSize = buffer.capacity();
//		if (size > currentSize) {
//			buffer = srb.extendBy(recId.getPosn(), size);
//		} else if (size < currentSize) {
//			buffer = srb.reduceBy(recId.getPosn(), size);
//		}
//		return buffer;
//	}
//
//	private ChannelBuffer createNewRecord(SerializedRecordBlock srb, int posn,
//			int size, long reserved,
//			CacheHandler<SerializedBlock> cacheHandler,
//			CacheResourceProvider<SerializedBlock> resourceProvider,
//			byte recordType) {
//		recordChunk = srb.createNewPosn(posn, size, reserved, recordType);
//		this.reserved = reserved;
//		return recordChunk.getDataBuffer();
//	}
//
//	private ChannelBuffer getDataBuffer() {
//		return srb.get(recId.getPosn());
//	}
//
//	@Override
//	public RecordId getRecordId() {
//		return recId;
//	}
//
//	@Override
//	public int getMaxContiguousFreeSize() {
//		return srb.getFreeSize();
//	}
//
//	@Override
//	public void remove(ColumnType ct) {
//		int posn = columns.lastIndexOf(ct);
//		if (posn >= 0) {
//			columns.remove(posn);
//			SerializableType st = values.remove(posn);
//			int size = st.getByteSize();
//			
//			srb.reduceBy(recId.getPosn(), getDataBuffer().capacity()-size);
//		}
//	}
//	
//	@Override
//	public SerializableType getValue(ColumnType ct) {
//		int posn = columns.lastIndexOf(ct);
//		if (posn >= 0) {
//			SerializableType st = values.get(posn);
//			return (DBType) st;
//		}		
//		return null;
//	}
//	
//	@Override
//	public void update(List<ColumnType> columnsParam, List<? extends SerializableType> valuesParam) {
//		int currentSize = getDataBuffer().capacity();
//		int newSize = 0;
//		List<? extends SerializableType> valuesCopy = new ArrayList<SerializableType>(values);
//		List<ColumnType> columnsCopy = new ArrayList<ColumnType>(columns);
//		
//		for (int i = 0; i < columnsParam.size(); i++) {
//			SerializableType value = valuesParam.get(i);
//			ColumnType ct = columnsCopy.get(i);
//			
//			if (value == null) {
//				columns.remove(ct);
//				values.remove(value);
//			} else {
//				int p = columns.indexOf(ct);
//				if (p >= 0) {
//					values.set(p, value);
//				}
//				newSize = newSize + value.getByteSize();
//				
//			}
//		}
//	}
//	
//	@Override
//	public List<ColumnType> getColumns() {
//		return columns;
//	}
//	
//	@Override 
//	public List<SerializableType> getValues() {
//		return values;
//	}
}
