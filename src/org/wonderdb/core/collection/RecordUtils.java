package org.wonderdb.core.collection;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.ObjectListRecord;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;
import org.wonderdb.types.record.TableRecord;

public class RecordUtils {
	private static RecordUtils instance = new RecordUtils();
	
	private RecordUtils() {
	}
	
	public static RecordUtils getInstance() {
		return instance;
	}
	
	public TableRecord convertToExtended(TableRecord newRecord, Set<Object> pinnedBlocks, 
			TypeMetadata meta, int maxBlockSize, int blockUsedSize, byte fileId) {
		
		int recordSize = RecordSerializer.getInstance().getRecordSize(newRecord, meta);
		if ((maxBlockSize - blockUsedSize - recordSize) > 0) {
			return newRecord;
		}
		
		Map<Integer, DBType> map = newRecord.getColumnMap();
		Iterator<Integer> iter = map.keySet().iterator();
		boolean recordSizeChanging = false;
		while (iter.hasNext()) {
			int columnId = iter.next();
			int type = ((TableRecordMetadata) meta).getColumnIdTypeMap().get(columnId);
			DBType newColumn = newRecord.getColumnMap().get(columnId);
			int size = ColumnSerializer.getInstance().getObjectRealSize(newColumn, new ColumnSerializerMetadata(type));
			if (size > maxBlockSize) {
				List<BlockPtr> list = ExtendedUtils.getInstance().extend(fileId, pinnedBlocks);
				ExtendedColumn ec = new ExtendedColumn(newColumn, list);
				map.put(columnId, ec);
				recordSizeChanging = true;
			}
		}

		if (recordSizeChanging) {
			recordSize = RecordSerializer.getInstance().getRecordSize(newRecord, meta);
		}
		
		if ((maxBlockSize - blockUsedSize - recordSize) > 0) {
			return newRecord;
		} else {
			List<BlockPtr> list = ExtendedUtils.getInstance().extend(fileId, pinnedBlocks);
			ExtendedTableRecord etr = new ExtendedTableRecord(map, list);
			return etr;
		}
	}
	
	public TableRecord convertToExtended(TableRecord oldRecord, TableRecord newRecord, Set<Object> pinnedBlocks, 
			TypeMetadata meta, int blockUsedSize, int maxBlockSize, byte fileId) {
		TableRecord retRecord = oldRecord;
		Map<Integer, DBType> map = oldRecord.getColumnMap();
		TableRecordMetadata trsm = (TableRecordMetadata) meta;
		Iterator<Integer> iter = map.keySet().iterator();
		int oldRecordSize = RecordSerializer.getInstance().getRecordSize(oldRecord, meta);
		
		while (iter.hasNext()) {
			int columnId = iter.next();
			DBType oldColumn = map.get(columnId);
			if (newRecord.getColumnMap().containsKey(columnId)) {
				int type = trsm.getColumnIdTypeMap().get(columnId);
				DBType newColumn = newRecord.getColumnMap().get(columnId);
				newColumn = convertToExtendedColumn(oldColumn, newColumn, maxBlockSize, fileId, pinnedBlocks, new ColumnSerializerMetadata(type));
				map.put(columnId, newColumn);
			}
		}
				
		int newRecordSize = RecordSerializer.getInstance().getRecordSize(oldRecord, meta);
		int availableSize = maxBlockSize - blockUsedSize - oldRecordSize;
		if (oldRecord instanceof ExtendedTableRecord) {
		} else {
			if (newRecordSize > availableSize) {
				List<BlockPtr> list = ExtendedUtils.getInstance().extend(fileId, pinnedBlocks);
				retRecord = new ExtendedTableRecord(oldRecord.getColumnMap(), list);
				retRecord.setRecordId(oldRecord.getRecordId());
			}
		}
		return retRecord;
	}
	
	public DBType convertToExtendedColumn(DBType oldColumn, DBType newColumn, int maxBlockSize, byte fileId, 
			Set<Object> pinnedBlocks, TypeMetadata meta) {
		
		int newColumnSize = ColumnSerializer.getInstance().getObjectSize(newColumn, meta);
		
		if (oldColumn instanceof ExtendedColumn) {
			if (newColumnSize < maxBlockSize) {
				ExtendedUtils.getInstance().releaseExtended((ExtendedColumn) oldColumn);
				return newColumn;
			} else {
				((ExtendedColumn) oldColumn).setValue(newColumn);
				return oldColumn;
			}
		} else {
			if (newColumnSize >= maxBlockSize) {
				List<BlockPtr> ptrList = ExtendedUtils.getInstance().extend(fileId, pinnedBlocks); 
				ExtendedColumn ec = new ExtendedColumn(newColumn, ptrList);
				return ec;
			}
		}
		
		return newColumn;
	}
	
	public ObjectRecord convertToExtended(ObjectRecord record, Set<Object> pinnedBlocks, TypeMetadata meta, 
			int availableSize, byte fileId) {
		ObjectRecord retRecord = record;
		DBType column = record.getColumn();
		int size = ColumnSerializer.getInstance().getObjectSize(column, meta);
		if (size >= availableSize) {
			List<BlockPtr> list = ExtendedUtils.getInstance().extend(fileId, pinnedBlocks);
			ExtendedColumn ec = new ExtendedColumn(column, list);
			retRecord.setColumn(ec);
		}
		return retRecord;
	}
	
	public int getConsumedResources(TableRecord record) {
		int consumedResources = 0;
		if (record instanceof Extended) {
			consumedResources = consumedResources + ((Extended) record).getPtrList().size();
		}
		
		Iterator<DBType> iter = record.getColumnMap().values().iterator();
		while (iter.hasNext()) {
			DBType c = iter.next();
			if (c instanceof Extended) {
				consumedResources = consumedResources + ((Extended) c).getPtrList().size();
			}
		}
		return consumedResources;
	}
	
	public int getConsumedResources(ObjectListRecord record) {
		int consumedResoures = 0;
		DBType c = record.getColumn();
		if (c instanceof Extended) {
			consumedResoures = consumedResoures + ((Extended) c).getPtrList().size();
		}
		return consumedResoures;
	}
	
	public int getConsumedResources(ListRecord record) {
		if (record instanceof TableRecord) {
			return getConsumedResources((TableRecord) record);
		}
		
		return getConsumedResources((ObjectListRecord) record);
	}

	public void mergeRecord(ListRecord oldRecord, ListRecord newRecord) {
		if (oldRecord instanceof IndexRecord) {
			IndexRecord iro = (IndexRecord) oldRecord;
			IndexRecord irn = (IndexRecord) newRecord;
			iro.setColumn(irn.getColumn());
			return;
		}
		
		TableRecord tro = (TableRecord) oldRecord;
		TableRecord trn = (TableRecord) newRecord;
		
		Iterator<Integer> iter = trn.getColumnMap().keySet().iterator();
		while (iter.hasNext()) {
			int colId = iter.next();
			DBType newColumn = trn.getColumnMap().get(colId);
			DBType oldColumn = tro.getColumnMap().get(colId);
			if (oldColumn instanceof ExtendedColumn) {
				((ExtendedColumn) oldColumn).setValue(newColumn);
			} else {
				tro.getColumnMap().put(colId, newColumn);
			}
		}
	}

	public void releaseRecord(Record record) {
		if (record instanceof TableRecord) {
			releaseTableRecord((TableRecord) record);
		} else if (record instanceof ObjectRecord) {
			releaseObjectRecord((ObjectListRecord) record);
		}
	}
	
	private void releaseTableRecord(TableRecord record) {
		Iterator<DBType> iter = record.getColumnMap().values().iterator();
		while (iter.hasNext()) {
			DBType column = iter.next();
			if (column instanceof Extended) {
				ExtendedUtils.getInstance().releaseExtended((Extended) column);
			}
		}
		if (record instanceof Extended) {
			ExtendedUtils.getInstance().releaseExtended((Extended) record);
		}
	}
	
	private void releaseObjectRecord(ObjectRecord record) {
		DBType column = record.getColumn();
		if (column instanceof Extended) {
			ExtendedUtils.getInstance().releaseExtended((Extended) column);
		}
	}
}
