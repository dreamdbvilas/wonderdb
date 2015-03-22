package org.wonderdb.serialize.record;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.helper.LazyExtendedSpaceProvider;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.block.BlockHeader;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedObjectListRecord;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.ObjectListRecord;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.RecordHeader;

public class ObjectRecordSerializer {
	public static ObjectRecordSerializer instance = new ObjectRecordSerializer();
	
	private ObjectRecordSerializer() {
	}
	
	public static ObjectRecordSerializer getInstance() {
		return instance;
	}
	
	public ObjectRecord readMinimum(BlockHeader blockHeader, ChannelBuffer buffer, TypeMetadata meta) {
		RecordHeader header = RecordHeaderSerializer.getInstance().getHeader(buffer);
		ObjectRecord record = null;
		
		if (header.isExtended()) {
			BlockPtr ptr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);
			List<BlockPtr> extendedPtrs = new ArrayList<BlockPtr>();
			extendedPtrs.add(ptr);
			if (blockHeader.isIndexBlock()) {
				record = new ExtendedObjectListRecord(null, extendedPtrs);
			}
			return record;
		}
		if (!blockHeader.isIndexBlock() && !blockHeader.isIndexBranchBlock()) {
			record = new ObjectListRecord(null);
		} else {
			record = new IndexRecord();
		}
		DBType column = ColumnSerializer.getInstance().readMinimum(buffer, meta);
		record.setColumn(column);
		return record;
	}
	
	public void readFull(ObjectRecord record, TypeMetadata meta) {
		
		ChannelBuffer buf = null;
		Set<Object> pinnedBlocks = new HashSet<>();
		try {
			if (record instanceof Extended) {
				buf = LazyExtendedSpaceProvider.getInstance().provideSpaceToRead(((Extended) record).getPtrList(), pinnedBlocks);
			} else {
				throw new RuntimeException("readFull is not required for non extended columns");
			}
	
			DBType column = ColumnSerializer.getInstance().readMinimum(buf, meta);
			record.setColumn(column);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
//	public void serializeExtended(byte fileId, ExtendedObjectListRecord record, int maxBlockSize, TypeMetadata meta, Set<Object> pinnedBlocks) {
//			int size = 1+ ColumnSerializer.getInstance().getObjectSize(record.getColumn(), meta);
//			size = size + ((record instanceof ObjectListRecord) ? 9 : 0);
//			int blocksRequired = getExtraBlocksRequired(size, maxBlockSize);
//			List<BlockPtr> list = record instanceof Extended ? ((Extended) record).getPtrList() : new ArrayList<>();
//			ChannelBuffer buf = LazyExtendedSpaceProvider.getInstance().provideSpaceToWrite(fileId, list, blocksRequired, pinnedBlocks);
//			RecordHeader header = new RecordHeader();
//			if (blocksRequired > 0) {
//				header.setExtended(true);
//				RecordHeaderSerializer.getInstance().serialize(header, buf);
//				DefaultSerializer.getInstance().serialize(list.get(0), buf, null);
//			} else {
//				RecordHeaderSerializer.getInstance().serialize(header, buf);
//			}
//			ColumnSerializer.getInstance().serializeFull(fileId, record.getColumn(), buf, maxBlockSize, meta, pinnedBlocks);
//	}
	
	public void serializeMinimum(ObjectRecord record, ChannelBuffer buffer, TypeMetadata meta) {
		RecordHeader header = new RecordHeader();
		header.setExtended(record instanceof Extended);
		RecordHeaderSerializer.getInstance().serialize(header, buffer);
		
		if (record instanceof Extended) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, ((Extended) record).getPtrList().get(0), buffer, meta);
		} else {
			writeMinimum(record, buffer, meta);
		}
	}
	
	private void writeMinimum(ObjectRecord record, ChannelBuffer buffer, TypeMetadata meta) {
		
		if (record instanceof Extended) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, ((Extended) record).getPtrList().get(0), buffer, meta);
			return;
		} else {
			ColumnSerializer.getInstance().serializeMinimum(((ObjectRecord) record).getColumn(), buffer, meta);
		}
	}

	public DBType getObject(BlockHeader header, ChannelBuffer buffer, TypeMetadata meta) {
		return readMinimum(header, buffer, meta);
	}

	public void serialize(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		serializeMinimum((ObjectRecord) object, buffer, meta);
	}

	public int getObjectSize(DBType object, TypeMetadata meta) {
		if (object instanceof Extended) {
			return 1+9;
		}
		return 1+ColumnSerializer.getInstance().getObjectSize(((ObjectRecord) object).getColumn(), meta);
	}
}
