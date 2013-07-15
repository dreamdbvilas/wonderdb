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


public class TempMultiBlockSerializer {
//
//	private void add(DataRecord record, RecordBlock block, CacheHandler handler, 
//			CacheResourceProvider<ExternalReference<BlockPtr, BlockBuffer>> resourceProvider) {
//		
//		int baseHeaderSize = getHeaderBaseSize(record);
//		int baseValueSize = getValueBaseSize(record);
//		int recordSize = baseHeaderSize + baseValueSize;
//		
//		ExternalReference<BlockPtr, BlockBuffer> ref = (ExternalReference<BlockPtr, BlockBuffer>) handler.getIgnorePin(block.getPtr());
//		int blockSize = ref.getByteSize();
//		int totalBlockSize = StorageUtils.getInstance().getTotalBlockSize(block.getPtr());
//		int freeSize = totalBlockSize - blockSize;
//
//		
//		if (freeSize > recordSize) {
//			serializeSingleBlockRecord(record, ref.getData(), block.getCurrentRecordPosn());
//		} else if (recordSize < totalBlockSize) {
//			RecordBlock rb = RecordFactory.getInstance().createRecordBlock(block.getSchemaObjectId(), changedBlocks, pinnedBlocks);
//			ref = resourceProvider.getResource(rb, System.currentTimeMillis());
//			serializeSingleBlockRecord(record, ref.getData(), rb.getCurrentRecordPosn());
//		} else {
//			Map<List<Object>, Integer> map = separateColumns(record, block.getSchemaObjectId(), block.getPtr());
//		}
//	}
//	
//	private void update(DataRecord record, RecordBlock block, Map<Object, DBType> changedColumns, CacheHandler handler, 
//			CacheResourceProvider<ExternalReference<BlockPtr, BlockBuffer>> resourceProvider) {
//		DataRecord storedRecord = unmarshalChangedColumns(record, block, changedColumns);
//		// if current record relocated.
//		// if yes
//		Set<Object> set = getColumnsNotFitting(storedRecord, changedColumns);
//		// for each column not fitting see if it can be fit in existing blocks.
//		// if not, lets group them together and fit them in one or more blocks.
//	}
//	
//	
//	private Map<List<Object>, Integer> separateColumns(DataRecord record, int schemaId, BlockPtr ptr) {
//		int startSize = BlockPtrSerializer.BASE_SIZE + Integer.SIZE/8;
//		int initialSize = 0;
//		
//		Map<List<Object>, Integer> retMap = new HashMap<List<Object>, Integer>();
//		Iterator<Object> iter = record.getColumns().keySet().iterator();
//		int blockSize = StorageUtils.getInstance().getTotalBlockSize(ptr);
//		int runningSize = initialSize;
//		List<Object> runningList = new ArrayList<Object>();
//		while (iter.hasNext()) {
//			Object key = iter.next();
//			DBType dt = record.getColumns().get(key);
//			
//			int size = dt.getByteSize();
//			if (size >= blockSize) {
//				List<Object> l = new ArrayList<Object>();
//				l.add(key);
//				retMap.put(l, size);
//				continue;
//			}
//			
//			if (size + runningSize > blockSize) {
//				retMap.put(runningList, runningSize);
//				runningSize = initialSize;
//				runningList = new ArrayList<Object>();
//			} else {
//				runningSize = runningSize + size;
//				runningList.add(key);
//			}
//		}
//		
//		if (runningList.size() > 0) {
//			retMap.put(runningList, runningSize);
//		}
//		
//		return retMap;
//	}
//	
//	int getHeaderBaseSize(DataRecord record) {
//		int size = 1; // byte indicating everything fits in one block
//		size = size + 1; // byte indicating if deleted.
//		Iterator<Object> iter = record.getColumns().keySet().iterator();
//		while (iter.hasNext()) {
//			Object key = iter.next();
//			size = size + 1 + Integer.SIZE/8;
//			if (key instanceof String) {
//				size = key.toString().getBytes().length;
//			}
//		}
//		
//		return size;
//	}
//	
//	int getValueBaseSize(DataRecord record) {
//		int size = 0;
//		Iterator<DBType> iter = record.getColumns().values().iterator();
//		while (iter.hasNext()) {
//			DBType dt = iter.next();
//			size = size + dt.getByteSize();
//		}
//		return size;
//	}
}
