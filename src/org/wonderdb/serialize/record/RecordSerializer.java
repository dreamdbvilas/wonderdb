package org.wonderdb.serialize.record;

import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.serialize.block.BlockHeader;
import org.wonderdb.types.DBType;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;
import org.wonderdb.types.record.TableRecord;

public class RecordSerializer {
	private static RecordSerializer instance = new RecordSerializer();
	
	private RecordSerializer() {
	}
	
	public static RecordSerializer getInstance() {
		return instance;
	}
	
	public Record readMinimum(BlockHeader blockHeader, ChannelBuffer buffer, TypeMetadata meta) {
		if (meta instanceof TableRecordMetadata) {
			return TableRecordSerializer.getInstance().readMinimum(buffer, meta);
		} 
		return ObjectRecordSerializer.getInstance().readMinimum(blockHeader, buffer, meta);		
	}
	
	public void readFull(Record record, TypeMetadata meta, Set<Object> pinnedBlocks) {
		if (meta instanceof TableRecordMetadata) {
			TableRecordSerializer.getInstance().readFull((TableRecord) record, meta, pinnedBlocks);
		} else {
			ObjectRecordSerializer.getInstance().readFull((ObjectRecord) record, meta, pinnedBlocks);
		}
	}
	
	public void serializeExtended(byte fileId, ExtendedTableRecord record,  int blockSize, TypeMetadata meta, Set<Object> pinnedBlocks) {
		TableRecordSerializer.getInstance().serializeExtended(fileId, record, blockSize, meta, pinnedBlocks);
	}
	
	public void serializeMinimum(Record record, ChannelBuffer buffer, TypeMetadata meta) {
		if (meta instanceof TableRecordMetadata) {
			TableRecordSerializer.getInstance().serializeMinimum((TableRecord) record, buffer, meta);
		} else {
			ObjectRecordSerializer.getInstance().serializeMinimum((ObjectRecord) record, buffer, meta);
		}
	}
	
	public int getRecordSize(DBType object, TypeMetadata meta) {
		if (meta instanceof TableRecordMetadata) {
			return TableRecordSerializer.getInstance().getObjectSize(object, meta);
		}
		return ObjectRecordSerializer.getInstance().getObjectSize(object, meta);		
	}
}
