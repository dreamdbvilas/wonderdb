package org.wonderdb.types.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.ListBlock;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TableRecordMetadata;

public class RecordManager {
	private static RecordManager instance = new RecordManager();
	
	private RecordManager() {
	}
	
	public static RecordManager getInstance() {
		return instance;
	}
	
	public BlockAndRecord getTableRecordAndLock(RecordId recordId, List<Integer> columnIds, TableRecordMetadata meta, Set<Object> pinnedBlocks) {
		ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, pinnedBlocks);
		block.readLock();
		try {
			TableRecord record = (TableRecord) block.getRecord(recordId.getPosn());
			if (record == null) {
				return null;
			}
			
			int currentResourceCount = record.getResourceCount();
			if (record instanceof ExtendedTableRecord && (record.getColumnMap() == null || record.getColumnMap().size() == 0)) {
				RecordSerializer.getInstance().readFull(record, meta);
			}
			
			if (columnIds == null) {
				columnIds = new ArrayList<Integer>(meta.getColumnIdTypeMap().keySet());
			}
			
			for (int i = 0; i < columnIds.size(); i++) {
				int columnId = columnIds.get(i);
				DBType column = record.getColumnMap().get(columnId);
				if (column instanceof ExtendedColumn) {
					((ExtendedColumn) column).getValue(new ColumnSerializerMetadata(meta.getColumnIdTypeMap().get(columnId)));
				}
	 		}
			BlockAndRecord bar = new BlockAndRecord();
			bar.block = block;
			if (record != null) {
				bar.record = (TableRecord) record.copyOf();
			}
			block.adjustResourceCount(record.getResourceCount()-currentResourceCount);
			return bar;
		} finally {
			if (block != null) {
				block.readUnlock();
			}
		}
	}
	
	public static class BlockAndRecord {
		public Block block = null;
		public Record record = null;
	}
}
