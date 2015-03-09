package org.wonderdb.serialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.helper.LazyExtendedSpaceProvider;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnHeader;
import org.wonderdb.types.ColumnSerializerMetadata;
//import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexRecordMetadata;
import org.wonderdb.types.NullType;
import org.wonderdb.types.TypeMetadata;

public class ColumnSerializer {
	public static ColumnSerializer instance = new ColumnSerializer();
	
	private ColumnSerializer() {
	}
	
	static public ColumnSerializer getInstance() {
		return instance;
	}
	
	public DBType readMinimum(ChannelBuffer buffer, TypeMetadata meta) {
		ColumnHeader header = ColumnHeaderSerializer.getInstance().getHeader(buffer);
		if (header.isNull()) {
			return NullType.getInstance();
		}
		
		DBType retVal = null;
		
		if (header.isExtended()) {
			BlockPtr ptr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);
			List<BlockPtr> extendedPtrs = new ArrayList<BlockPtr>();
			extendedPtrs.add(ptr);
			retVal = new ExtendedColumn(null, extendedPtrs);
		} else {
			int type = -1;
			if (meta instanceof ColumnSerializerMetadata){
				type = ((ColumnSerializerMetadata) meta).getColumnId();
			} else if (meta instanceof IndexRecordMetadata) {
				type = SerializerManager.INDEX_TYPE;
			}
			retVal = Serializer.getInstance().getObject(type, buffer, meta);
		}
		return retVal;
	}
	
	public void readFull(DBType column, TypeMetadata meta, Set<Object> pinnedBlocks) {
		ChannelBuffer buf = null;
		if (column instanceof Extended) {
			buf = LazyExtendedSpaceProvider.getInstance().provideSpaceToRead(((Extended) column).getPtrList(), pinnedBlocks);
		} else {
			throw new RuntimeException("readFull should not be required for non extended columns");
		}
		
		int type = -1;
		if (meta instanceof ColumnSerializerMetadata){
			type = ((ColumnSerializerMetadata) meta).getColumnId();
		} else if (meta instanceof IndexRecordMetadata) {
			type = SerializerManager.INDEX_TYPE;
		}
		DBType dt = Serializer.getInstance().getObject(type, buf, meta);
		((ExtendedColumn) column).setValue(dt);
	}
	
	public void serializeExtended(byte fileId, ExtendedColumn column, int blockSize, TypeMetadata meta, Set<Object> pinnedBlocks) {
		int type = -1;
		if (meta instanceof ColumnSerializerMetadata){
			type = ((ColumnSerializerMetadata) meta).getColumnId();
		} else if (meta instanceof IndexRecordMetadata) {
			type = SerializerManager.INDEX_TYPE;
		}
		int size = 1+ Serializer.getInstance().getObjectSize(type, column.getValue(), meta);
		int blocksRequired = getExtraBlocksRequired(size, blockSize);
		List<BlockPtr> list = column instanceof ExtendedColumn ? ((ExtendedColumn) column).getPtrList() : new ArrayList<>();
		ChannelBuffer buf = LazyExtendedSpaceProvider.getInstance().provideSpaceToWrite(fileId, list, blocksRequired, pinnedBlocks);
		ColumnHeader header = new ColumnHeader();
		if (column.getValue() == NullType.getInstance()) {
			header.setNull(true);
		}
		ColumnHeaderSerializer.getInstance().serialize(header, buf);
		Serializer.getInstance().serialize(type, column.getValue(), buf, meta);
	}
	
	public void serializeMinimum(DBType column, ChannelBuffer buffer, TypeMetadata meta) {
		ColumnHeader header = new ColumnHeader();
		header.setExtended(column instanceof Extended);
		header.setNull(column == NullType.getInstance());
		ColumnHeaderSerializer.getInstance().serialize(header, buffer);
		int type = -1;
		if (meta instanceof ColumnSerializerMetadata){
			type = ((ColumnSerializerMetadata) meta).getColumnId();
		} else if (meta instanceof IndexRecordMetadata) {
			type = SerializerManager.INDEX_TYPE;
		}

		if (column instanceof Extended) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, ((Extended) column).getPtrList().get(0), buffer, meta);
		} else {
			Serializer.getInstance().serialize(type, column, buffer, meta);
		}
	}
	
	public int getObjectRealSize(DBType column, TypeMetadata meta) {
		int type = -1;
		if (meta instanceof ColumnSerializerMetadata){
			type = ((ColumnSerializerMetadata) meta).getColumnId();
		} else if (meta instanceof IndexRecordMetadata) {
			type = SerializerManager.INDEX_TYPE;
		}
		return 1+ Serializer.getInstance().getObjectSize(type, column, meta);
	}
	
	public int getObjectBlockSize(DBType column, TypeMetadata meta) {
		if (column instanceof Extended) {
			return 9+1;
		}
		
		return getObjectRealSize(column, meta);
	}
	
	private int getExtraBlocksRequired(int requiredSize, int maxSize) {
		int mSize = maxSize -10;
		int retSize = requiredSize/mSize;
		int balance = requiredSize%mSize;
		return balance > 0 ? retSize + 1 : retSize;
	}

	public DBType getObject(int type, ChannelBuffer buffer, TypeMetadata meta) {
		return readMinimum(buffer, meta);
	}

	public void serialize(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		serializeMinimum(object, buffer, meta);		
	}

	public int getObjectSize(DBType object, TypeMetadata meta) {
		int type = -1;
		if (meta instanceof ColumnSerializerMetadata){
			type = ((ColumnSerializerMetadata) meta).getColumnId();
		} else if (meta instanceof IndexRecordMetadata) {
			type = SerializerManager.INDEX_TYPE;
		}
		return 1 + (object instanceof Extended ? 9 : Serializer.getInstance().getObjectSize(type, object, meta)); 
	}
}
