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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.RecordHeaderType;


public class RecordLoader {
	// singleton
	private static RecordLoader instance = new RecordLoader();
	
	// singleton
	private RecordLoader() {
	}
	
	// singleton 
	public static RecordLoader getInstance() {
		return instance;
	}
	
//	public RecordHeaderType loadHeader(RecordId recordId, Set<BlockPtr> pinnedBlocks) {
//		SerializedMultiBlockChunk smbc = new SerializedMultiBlockChunk(recordId, pinnedBlocks);
//		ChannelBuffer buffer = smbc.getDataBuffer();
//		return RecordHeaderTypeSerializer.getInstance().unmarshal(buffer);
//	}
	
	public LoadedMapAndBufferCouut loadValues(SerializedRecordBlock srb, RecordId recId, RecordHeaderType rht, 
			List<ColumnType> columns, Map<RecordId, SerializedMultiBlockRecord> loadedValues, 
			int schemaId, Set<BlockPtr> pinnedBlocks, SerializedRecord sr) {
		
		Map<RecordId, List<ColumnType>> map = rht.getHeader();
		Iterator<RecordId> iter = map.keySet().iterator();
		Map<RecordId, SerializedMultiBlockRecord> retMap = new HashMap<RecordId, SerializedMultiBlockRecord>();
		int bufferCount = 0;
		while (iter.hasNext()) {
			RecordId recordId = iter.next();
			List<ColumnType> headerColumns = map.get(recordId);
			if (headerContains(headerColumns, columns) || columns == null || columns.size() == 0) {
				if (!loadedValues.containsKey(recordId)) {
					SerializedMultiBlockRecord smbr = null;
					if (recordId.getPtr().equals(recId.getPtr())) {
						smbr = new SerializedMultiBlockRecord(srb, recordId, map.get(recordId), 
								schemaId, pinnedBlocks, SerializedRecordChunk.DATA_CHUNK, sr);
					} else {
						smbr = new SerializedMultiBlockRecord(srb, recordId, map.get(recordId), 
								schemaId, pinnedBlocks, SerializedRecordChunk.DATA_ONLY_BLOCK, sr);
					}
					retMap.put(recordId, smbr);
					List<BlockPtr> recordBlockPtrs = smbr.getChangedBlocks();
					for (int i = 0; i < recordBlockPtrs.size(); i++) {
						if (!recId.getPtr().equals(recordBlockPtrs.get(i))) {
							bufferCount = StorageUtils.getInstance().getSmallestBlockCount(recId.getPtr());
						}
					}
					loadedValues.put(recordId, smbr);
				}
			}
		}
		CacheEntryPinner.getInstance().pin(recId.getPtr(), pinnedBlocks);
		return new LoadedMapAndBufferCouut(retMap, bufferCount);
	}
	
	private boolean headerContains(List<ColumnType> headerList, List<ColumnType> loadColumns) {
		if (loadColumns == null) {
			return true;
		}
		
		for (int i = 0; i < headerList.size(); i++) {
			ColumnType ct = headerList.get(i);
			if (loadColumns.contains(ct)) {
				return true;
			}
		}
		
		return false;
	}
	
	public static class LoadedMapAndBufferCouut {
		private Map<RecordId, SerializedMultiBlockRecord> map;
		private int bufferCount = 0;
		
		public LoadedMapAndBufferCouut(Map<RecordId, SerializedMultiBlockRecord> map, int bufferCount) {
			this.map = map;
			this.bufferCount = bufferCount;
		}
		
		public Map<RecordId, SerializedMultiBlockRecord> getLoadedMap() {
			return map;
		}
		
		public int getBufferCount() {
			return bufferCount;
		}
	}
}
