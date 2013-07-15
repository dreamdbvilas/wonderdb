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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.RecordHeaderTypeSerializer;
import org.wonderdb.seralizers.block.RecordLoader.LoadedMapAndBufferCouut;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.RecordHeaderType;


public class SerializedRecord {
	RecordHeaderType rht = null;
	Map<RecordId, SerializedMultiBlockRecord> values = new HashMap<RecordId, SerializedMultiBlockRecord>();
	int schemaId = -1;
	SerializedMultiBlockChunk headerChunk = null;
	SerializedRecordBlock headerBlock = null;
	SerializedRecordBlock valueBlock = null;
	boolean changedHeaderChunk = false;
	byte fileId;
	Set<BlockPtr> pinnedBlocks = null;
	public Map<BlockPtr, SerializedRecordBlock> srbMap = new HashMap<BlockPtr, SerializedRecordBlock>();
	
	List<SerializedRecordBlock> srbList = new ArrayList<SerializedRecordBlock>();
	
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();

	public SerializedRecord(SerializedRecordBlock headerBlock, SerializedRecordBlock valueBlock, int schemaId, Set<BlockPtr> pinnedBlocks) {
		this.schemaId = schemaId;
		this.headerBlock = headerBlock;
		this.fileId = headerBlock.getPtr().getFileId();
		this.valueBlock = valueBlock;
		this.pinnedBlocks = pinnedBlocks;
		if (headerBlock != null) {
			srbMap.put(headerBlock.getPtr(), headerBlock);
		}
		if (valueBlock != null) {
			srbMap.put(valueBlock.getPtr(), valueBlock);
		}
	}
	
	public SerializedRecord(SerializedRecordBlock srb, int schemaId, Set<BlockPtr> pinnedBlocks) {
		this (srb, srb, schemaId, pinnedBlocks);
	}
	
	public SerializedRecord(BlockPtr ptr, int schemaId, Set<BlockPtr> pinnedBlocks) {
		SerializedBlock ref = null;
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		ref = (SerializedBlock) secondaryCacheHandler.get(ptr);
		
		SerializedBlock serializedBlock = ref;
		SerializedRecordLeafBlock srb = null;
//		synchronized (serializedBlock) {
			srb = new SerializedRecordLeafBlock(serializedBlock, false);
			srbMap.put(srb.getPtr(), srb);
//		}

		this.schemaId = schemaId;
		headerBlock = srb;
		this.fileId = srb.getPtr().getFileId();
		this.pinnedBlocks = pinnedBlocks;
	}
	
	public RecordId getRecordId() {
		return headerChunk != null ? headerChunk.getRecordId() : null;
	}
	
	public boolean loadData(RecordId recId, List<ColumnType> columns) {
		if (loadHeader(recId)) {
			loadColumns(recId, columns);
			return true;
		}
		return false;
	}
	
	private boolean loadHeader(RecordId recId) {
		headerChunk = new SerializedMultiBlockChunk(schemaId, headerBlock, recId, pinnedBlocks, SerializedRecordChunk.HEADER_CHUNK, this);
		if (headerChunk == null) {
			return false;
		}
		ChannelBuffer buffer = headerChunk.getDataBuffer();
		rht = RecordHeaderTypeSerializer.getInstance().unmarshal(buffer);
		return true;
	}
	
	private void loadColumns(RecordId recordId, List<ColumnType> columns) {
		LoadedMapAndBufferCouut lmbc = RecordLoader.getInstance().loadValues(valueBlock, recordId, rht, columns, values, schemaId, pinnedBlocks, this);
		values = lmbc.getLoadedMap();
	}
	
	public SerializableType getValue(ColumnType ct) {
		SerializedMultiBlockRecord smbr = null;
		smbr = getMultiBlockRecord(ct);
		if (smbr == null) {
			return null;
		}
		
		return smbr.getValue(ct);
	}
	
	private SerializedMultiBlockRecord getMultiBlockRecord(ColumnType ct) {
		if (rht == null) {
			return null;
		}
		
		Iterator<RecordId> iter = rht.getHeader().keySet().iterator();
		while (iter.hasNext()) {
			RecordId recId = iter.next();
			List<ColumnType> list = rht.getHeader().get(recId);
			if (list.contains(ct)) {
				if (recId.getPtr().equals(getRecordId().getPtr())) {
					return new SerializedMultiBlockRecord(valueBlock, recId, list, schemaId, pinnedBlocks, SerializedRecordChunk.DATA_CHUNK, this);
				} else {
					return new SerializedMultiBlockRecord(valueBlock, recId, list, schemaId, pinnedBlocks, SerializedRecordChunk.DATA_ONLY_BLOCK, this);
				}
			}
		}
		
		return null;
	}
	
	public void updateColumns(Map<ColumnType, ? extends SerializableType> changedColumnsMap) {
		if (rht == null) {
			rht = new RecordHeaderType();
		}
		Set<SerializedMultiBlockRecord> set = new HashSet<SerializedMultiBlockRecord>();
		Iterator<ColumnType> iter = changedColumnsMap.keySet().iterator();
		while (iter.hasNext()) {
			ColumnType ct = iter.next();
			SerializableType value = changedColumnsMap.get(ct);
			Set<SerializedMultiBlockRecord> s = updateValue(ct, value);
			if (s != null) {
				set.addAll(s);
			}
		}
		
		if (headerChunk == null) {
			SerializedRecordBlock srb = headerBlock;
			headerChunk = new SerializedMultiBlockChunk(schemaId, srb, rht.getByteSize(), 0, 
					SerializedRecordChunk.HEADER_CHUNK, pinnedBlocks, this);
			changedHeaderChunk = true;
		} else {
			headerChunk.resize(rht.getByteSize());
			changedHeaderChunk = true;
		}
		Iterator<SerializedMultiBlockRecord> iter1 = set.iterator();
//		RecordId recId = headerChunk.getRecordId();
//		boolean headerBuilt = false;
		if (changedHeaderChunk) {
			RecordHeaderTypeSerializer.getInstance().toBytes(rht, headerChunk.getDataBuffer());
		}		
		while (iter1.hasNext()) {
			SerializedMultiBlockRecord srmb = iter1.next();
			srmb.serialize();
//			srmb.rebuild();
//			if (headerChunk.getRecordId().getPtr().equals(srmb.getRecordId().getPtr())) {
//				headerBuilt = true;
//			}
		}
		
//		List<SerializedRecordBlock> srbList = new ArrayList<SerializedRecordBlock>();
//		iter1 = set.iterator();
//		while (iter1.hasNext()) {
//			SerializedMultiBlockRecord srmb = iter1.next();
//			srbList.add(srmb.getRecordBlock());
//		}
//		srbList.add(headerChunk.getRecordBlock());
//		
		Iterator<SerializedRecordBlock> itr = srbMap.values().iterator();
		int c = 0;
		while(itr.hasNext()) {
			itr.next().buildDataBuffer();
			c++;
		}
		if (c == 0) {
			Logger.getLogger(getClass()).fatal("no buffer to rebuild after updateColumns");
		}
		headerBlock.buildDataBuffer();
		if (valueBlock != headerBlock) {
			valueBlock.buildDataBuffer();
		}
	}
	
	public Set<BlockPtr> getChangedRecordBlocks() {
		Set<BlockPtr> retSet = new HashSet<BlockPtr>();
		if (headerChunk != null) {
			retSet.addAll(headerChunk.getChangedBlocks());
		}
		Iterator<RecordId> iter = values.keySet().iterator();
		while (iter.hasNext()) {
			RecordId recordId = iter.next();
			SerializedMultiBlockRecord smbr = values.get(recordId);
			retSet.addAll(smbr.getChangedBlocks());
		}
		return retSet;
	}
	
	public Map<ColumnType, DBType> getAllColumns() {
		Map<ColumnType, DBType> retMap = new HashMap<ColumnType, DBType>();
		Map<RecordId, List<ColumnType>> allColumns = rht.getHeader();
		Iterator<List<ColumnType>> iter = allColumns.values().iterator();
		while (iter.hasNext()) {
			List<ColumnType> list = iter.next();
			List<SerializableType> values = null;
			SerializedMultiBlockRecord smbr = getMultiBlockRecord(list.get(0));
			if (smbr != null) {
				values = smbr.getValues();
			}
			for (int i = 0; i < list.size(); i++) {
				ColumnType ct = list.get(i);
				DBType value = (DBType) (values != null ? values.get(i) : null);
				retMap.put(ct, value);
			}
		}
		return retMap;
	}
	
	public int getLoadedBufferCount() {
		int count = 0;
		Iterator<SerializedMultiBlockRecord> iter = values.values().iterator();
		while (iter.hasNext()) {
			SerializedMultiBlockRecord smbr = iter.next();
			count = count + smbr.getChangedBlockCount();
		}
		return count;
	}
	
	public void remove() {
		
		if (rht == null) {
			return;
		}
		RecordId recordId = headerChunk.getRecordId();
		List<BlockPtr> changedBlockList = headerChunk.getChangedBlocks();
		boolean reloadHeader = false;
		Iterator<SerializedMultiBlockRecord> iter = values.values().iterator();
		while (iter.hasNext()) {
			SerializedMultiBlockRecord smbr = iter.next();
			if (smbr.getRecordId().getPtr().equals(headerChunk.getRecordId().getPtr())) {
				reloadHeader = true;
			}
			smbr.remove(recordId.getPtr());
			changedBlockList.addAll(smbr.getChangedBlocks());			
		}

		if (reloadHeader) {
			headerChunk = new SerializedMultiBlockChunk(schemaId, headerBlock, recordId, pinnedBlocks, SerializedRecordChunk.HEADER_SIZE, this);
		}
		SerializedRecordBlock hrb = headerChunk.getFirstRecordBlock();
		headerChunk.remove(recordId.getPtr());
		hrb.buildDataBuffer();
		changedBlockList.addAll(headerChunk.getChangedBlocks());
		if (changedBlockList != null) {
			for (int i = 0; i < changedBlockList.size(); i++) {
				BlockPtr ptr = changedBlockList.get(i);
				secondaryCacheHandler.changed(ptr);
			}
		}
	}
	
	private Set<SerializedMultiBlockRecord> updateValue(ColumnType ct, SerializableType value) {
		Set<SerializedMultiBlockRecord> set = new HashSet<SerializedMultiBlockRecord>();
		
		SerializedMultiBlockRecord srmb = findBlockRecord(ct, value);
		boolean relocate = false;
		
		if (value == null) {
			if (srmb != null) {
				srmb.remove(ct);
				rht.addDataChunk(srmb.getRecordId(), srmb.getColumns());
				set.add(srmb);
				relocate = true;
				return set;
			} else {
				return null;
			}
		}
		
		List<ColumnType> cList = new ArrayList<ColumnType>();
		cList.add(ct);
		List<SerializableType> valueList = new ArrayList<SerializableType>();
		valueList.add(value);

		if (srmb == null) {
			srmb = getBestFit(ct, value);
			changedHeaderChunk = true;
		} else {
			if (!willFit(srmb, ct, value)) {
				srmb.remove(ct);
				rht.addDataChunk(srmb.getRecordId(), srmb.getColumns());
				set.add(srmb);
				srmb = getBestFit(ct, value);
				changedHeaderChunk = true;
				relocate = true;
			}
		}
		
		if (srmb != null) {
			srmb.update(cList, valueList);
			set.add(srmb);
			if (relocate) {
				rht.addDataChunk(srmb.getRecordId(), srmb.getColumns());
				changedHeaderChunk = true;
			}
		} else {
			srmb = createAndUpdate(ct, value);
			set.add(srmb);
			rht.addDataChunk(srmb.getRecordId(), srmb.getColumns());
			values.put(srmb.getRecordId(), srmb);
			changedHeaderChunk = true;
		}
		return set;
	}
	
	private SerializedMultiBlockRecord findBlockRecord(ColumnType ct, SerializableType st) {
		RecordId recordId = rht.find(ct);
		return values.get(recordId);
	}
	
	private boolean willFit(SerializedMultiBlockRecord srmb, ColumnType ct, SerializableType st) {
		int newSize = 0;
		if (st != null) {
			newSize = st.getByteSize();
		}
		int maxChunkSize = StorageUtils.getInstance().getMaxChunkSize(srmb.getRecordId().getPtr());
		if (newSize > maxChunkSize && srmb.getColumns().size() == 1) {
			return true;
		}
		int currentSize = srmb.getValue(ct).getByteSize();
		int freeSize = srmb.getTotalFreeSize();
		if (freeSize > newSize-currentSize) {
			return true;
		}
		return false;
	}
	
	private SerializedMultiBlockRecord getBestFit(ColumnType ct, SerializableType value) {
		SerializedMultiBlockRecord srmb = null;
		if (value == null) {
			return null;
		}
		
		int valueSize = value.getByteSize();
		
		Iterator<RecordId> iter = rht.getHeader().keySet().iterator();
		while (iter.hasNext()) {
			RecordId rId = iter.next();
			srmb = values.get(rId);
			
			if (srmb != null && srmb.getChangedBlocks().size() == 1 && srmb.getMaxContiguousFreeSize() > valueSize) {
				return srmb;
			}
		}
		return null;
	}
	
	private SerializedMultiBlockRecord createAndUpdate(ColumnType ct, SerializableType value) {
		List<ColumnType> columnList = new ArrayList<ColumnType>();
		List<SerializableType> valueList = new ArrayList<SerializableType>();
		columnList.add(ct);
		valueList.add(value);
		
		if (valueBlock == null) {
			SerializedMultiBlockRecord smbr = new SerializedMultiBlockRecord(fileId, columnList, valueList, 
					schemaId, 0, SerializedRecordChunk.DATA_ONLY_BLOCK, pinnedBlocks, this);
			valueBlock = smbr.getRecordBlock();
			return smbr;
		}
		return new SerializedMultiBlockRecord(valueBlock, columnList, valueList, 
				schemaId, 0, SerializedRecordChunk.DATA_ONLY_BLOCK, pinnedBlocks, this);
	}
}
