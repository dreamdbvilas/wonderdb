package org.wonderdb.serialize.record;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.helper.LazyExtendedSpaceProvider;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.RecordHeader;
import org.wonderdb.types.record.TableRecord;

public class TableRecordSerializer  {
	public static TableRecordSerializer instance = new TableRecordSerializer();
	
	private TableRecordSerializer() {
	}
	
	public static TableRecordSerializer getInstance() {
		return instance;
	}
	
	public TableRecord readMinimum(ChannelBuffer buffer, TypeMetadata meta) {
		RecordHeader header = RecordHeaderSerializer.getInstance().getHeader(buffer);
		TableRecord record = null;
		
		if (header.isExtended()) {
			BlockPtr ptr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);
			List<BlockPtr> extendedPtrs = new ArrayList<BlockPtr>();
			extendedPtrs.add(ptr);
			record = new ExtendedTableRecord(new HashMap<>(), extendedPtrs);
			return record;
		}
		record = new TableRecord(null);
		
		readMinimumColumns(record, buffer, meta);
		return record;
	}
	
	private void readMinimumColumns(TableRecord record, ChannelBuffer buffer, TypeMetadata meta) {

		TableRecordMetadata trsb = (TableRecordMetadata) meta;
		int size = buffer.readInt();
		Map<Integer, DBType> map = new HashMap<>(size);
		
		for (int i = 0; i < size; i++) {
			int columnId = buffer.readInt();
			ColumnSerializerMetadata csm = new ColumnSerializerMetadata(trsb.getColumnIdTypeMap().get(columnId));
			DBType column = ColumnSerializer.getInstance().readMinimum(buffer, csm);
			map.put(columnId, column);
		}
		
		record.setColumnMap(map);
	}
	
	public void readFull(TableRecord record, TypeMetadata meta) {
		
		ChannelBuffer buf = null;
		Set<Object> pinnedBlocks = new HashSet<>();
		try {
			if (record instanceof Extended) {
				buf = LazyExtendedSpaceProvider.getInstance().provideSpaceToRead(((Extended) record).getPtrList(), pinnedBlocks);
			} else {
				throw new RuntimeException("readFull is not required for non extended columns");
			}
	
			readMinimumColumns(record, buf, meta);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public int getFullSize(TableRecord record, TypeMetadata meta) {
		int size = 1 + Integer.BYTES + 9;
		TableRecordMetadata trsm = (TableRecordMetadata) meta;
		Map<Integer, Integer> map = trsm.getColumnIdTypeMap();
		Iterator<Integer> iter = record.getColumnMap().keySet().iterator();
		while (iter.hasNext()) {
			int id = iter.next();
			int type = map.get(id);
			DBType column = record.getColumnMap().get(id);
			
			size = size + Integer.BYTES + ColumnSerializer.getInstance().getObjectSize(column, new ColumnSerializerMetadata(type));
		}
		return size;
	}
	
	public int getObjectBlockSize(TableRecord record, TypeMetadata meta) {
		if (record instanceof Extended) {
			return 9+1;
		}
		
		return getFullSize(record, meta);
	}
	
	public void serializeExtended(byte fileId, TableRecord record, int blockSize, TypeMetadata meta, Set<Object> pinnedBlocks) {
		
		int size = getFullSize(record, meta);
		
		int blocksRequired = getExtraBlocksRequired(size, blockSize);
		List<BlockPtr> list = record instanceof Extended ? ((Extended) record).getPtrList() : new ArrayList<>();
		ChannelBuffer buf = LazyExtendedSpaceProvider.getInstance().provideSpaceToWrite(fileId, list, blocksRequired, pinnedBlocks);
		RecordHeader header = new RecordHeader();
		if (blocksRequired > 0) {
			header.setExtended(true);
		}
		
		writeTableRecord(record, buf, meta);
	}
	
	public void serializeMinimum(TableRecord record, ChannelBuffer buffer, TypeMetadata meta) {
		RecordHeader header = new RecordHeader();
		header.setExtended(record instanceof Extended);
		RecordHeaderSerializer.getInstance().serialize(header, buffer);
		
		if (record instanceof Extended) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, ((Extended) record).getPtrList().get(0), buffer, meta);
		} else {
			writeMinimum(record, buffer, meta);
		}
	}
	
	private void writeMinimum(TableRecord record, ChannelBuffer buffer, TypeMetadata meta) {
		
		if (record instanceof Extended) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, ((Extended) record).getPtrList().get(0), buffer, meta);
			return;
		} else {
			writeTableRecord(record, buffer, meta);
		}
	}
	
	private void writeTableRecord(TableRecord record, ChannelBuffer buffer, TypeMetadata meta) {
		Map<Integer, Integer> map = ((TableRecordMetadata) meta).getColumnIdTypeMap();
		Map<Integer, DBType> columnMap = ((TableRecord) record).getColumnMap();
		
		buffer.writeInt(columnMap.size());
		Iterator<Integer> iter = columnMap.keySet().iterator();
		while (iter.hasNext()) {
			int colId = iter.next();
			buffer.writeInt(colId);
			DBType column = columnMap.get(colId);
			ColumnSerializerMetadata csm = new ColumnSerializerMetadata(map.get(colId));
			ColumnSerializer.getInstance().serializeMinimum(column, buffer, csm);
		}
	}

	private int getExtraBlocksRequired(int requiredSize, int maxSize) {
		int mSize = maxSize -10;
		int retSize = requiredSize/mSize;
		int balance = requiredSize%mSize;
		return balance > 0 ? retSize + 1 : retSize;
	}
	
	public int getObjectSize(DBType object, TypeMetadata meta) {
		if (object instanceof Extended) {
			return 9;
		}
		return getFullSize((TableRecord) object, meta);
	}
}
