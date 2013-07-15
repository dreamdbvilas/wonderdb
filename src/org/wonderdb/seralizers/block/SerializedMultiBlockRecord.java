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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.seralizers.RecordValuesTypeSerializer;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.RecordValuesType;


public class SerializedMultiBlockRecord implements SerializedRecordColumns {
	SerializedMultiBlockChunk record = null;
	long reserved;
	List<ColumnType> columnList = null;
	RecordValuesType rvt = null;
	int schemaId = -1;
	Set<BlockPtr> pinnedBlocks = null;
	SerializedRecord sr = null;
	
	public SerializedMultiBlockRecord(SerializedRecordBlock srb, RecordId recId, List<ColumnType> columnList, int schemaId, 
			Set<BlockPtr> pinnedBlocks, int recordType, SerializedRecord sr) {
		this.columnList = columnList;
		this.schemaId = schemaId;
		this.pinnedBlocks = pinnedBlocks;
		this.sr = sr;
		loadRecord(srb, recId, recordType);
	}
	
	public void rebuild() {
		record.rebuild();
	}
	
	public SerializedRecordBlock getRecordBlock() {
		return record != null ? record.getFirstRecordBlock() : null;
	}
	
	public SerializedMultiBlockRecord(byte fileId, List<ColumnType> columnList, List<SerializableType> values, 
			int schemaId, long reserved, byte chunkType, Set<BlockPtr> pinnedBlocks, SerializedRecord sr) {
		
		this.columnList = columnList;
		rvt = new RecordValuesType(values);
		int size = rvt.getByteSize();
		this.pinnedBlocks = pinnedBlocks;
		this.sr = sr;
		record = new SerializedMultiBlockChunk(schemaId, fileId, size, 0, chunkType, pinnedBlocks, sr);
		record.getDataBuffer().clear();
//		RecordValuesTypeSerializer.getInstance().toBytes(rvt, record.getDataBuffer());
		this.schemaId = schemaId;
		this.pinnedBlocks = pinnedBlocks;
	}
	
	public SerializedMultiBlockRecord(SerializedRecordBlock suggestedRecordBlock, List<ColumnType> columnList, List<SerializableType> values, 
			int schemaId, long reserved, byte chunkType, Set<BlockPtr> pinnedBlocks, SerializedRecord sr) {
		this.sr = sr;
		this.columnList = columnList;
		rvt = new RecordValuesType(values);
		int size = rvt.getByteSize();
		record = new SerializedMultiBlockChunk(schemaId, suggestedRecordBlock, size, 0, chunkType, pinnedBlocks, sr);
		record.getDataBuffer().clear();
//		RecordValuesTypeSerializer.getInstance().toBytes(rvt, record.getDataBuffer());
		this.schemaId = schemaId;
		this.pinnedBlocks = pinnedBlocks;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.block.SerializedRecordColumns#getRecordId()
	 */
	@Override
	public RecordId getRecordId() {
		return record != null ? record.getRecordId() : null;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.block.SerializedRecordColumns#getMaxContiguousFreeSize()
	 */
	@Override
	public int getMaxContiguousFreeSize() {
		return record.getFreeSize();
	}	
	
	@Override
	public int getTotalFreeSize() {
		return record.getFreeSize();
	}
	
	public void remove(ColumnType ct) {
		int posn = columnList.indexOf(ct);
		columnList.remove(ct);
		if (posn >= 0) {
			SerializableType st = rvt.getValues().remove(posn);
			if (st != null) {
				record.resize(rvt.getByteSize());
			}
		}
	}
	
	public void remove(BlockPtr rootBlockPtr) {
		record.remove(rootBlockPtr);
	}
	
	@Override
	public void update(List<ColumnType> columns, List<SerializableType> values) {
		int totalSize = rvt.getByteSize();
		
		for (int i = 0; i < columns.size(); i++) {
			ColumnType columnType = columns.get(i);
			SerializableType value = values.get(i);
			SerializableType currentValue = getValue(columnType);
			int currentValueSize = currentValue == null ? 0 : currentValue.getByteSize();
			
			if (value == null) {
				if (currentValue != null) {
					totalSize = totalSize - currentValue.getByteSize();
					remove(columnType);
				}
			} else {
				totalSize = totalSize + value.getByteSize() - currentValueSize;
				int p = columnList.lastIndexOf(columnType);
				if (p >= 0) {
					rvt.getValues().set(p, value);
				} else {
					columnList.add(columnType);
					rvt.getValues().add(value);
				}
			}
		}
		ChannelBuffer buffer = record.resize(totalSize);
		buffer.clear();
	}
	
	@Override
	public SerializableType getValue(ColumnType columnType) {
		int p = columnList.lastIndexOf(columnType);
		if (p >= 0) {
			return rvt.getValues().get(p);
		}
		return null;
	}

	public List<BlockPtr> getChangedBlocks() {
		if (record != null) {
			return record.getChangedBlocks();
		}
		return new ArrayList<BlockPtr>();
	}
	
	public int getChangedBlockCount() {
		return record != null ? record.getChangedBlockCount() : 0;
	}
	
	/* (non-Javadoc)
	 * @see com.mydreamdb.seralizers.block.SerializedRecordColumns#loadRecord(com.mydreamdb.block.record.manager.RecordId, com.mydreamdb.cache.CacheHandler)
	 */
	private void loadRecord(SerializedRecordBlock srb, RecordId recordId, int recordType) {
		record = new SerializedMultiBlockChunk(schemaId, srb, recordId, pinnedBlocks, recordType, sr);
		this.rvt = RecordValuesTypeSerializer.getInstance().unmarshal(record.getDataBuffer(), columnList, schemaId);
	}
	
	public void serialize() {
//		record.resize(rvt.getByteSize());
		RecordValuesTypeSerializer.getInstance().toBytes(rvt, record.getDataBuffer());
	}
	
	@Override
	public List<ColumnType> getColumns() {
		return columnList;
	}
	
	@Override
	public List<SerializableType> getValues() {
		return rvt.getValues();
	}
}
