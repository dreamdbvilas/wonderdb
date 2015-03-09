package org.wonderdb.types.record;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;

public class TableRecord implements ListRecord  {
	Map<Integer, DBType> columnMap = null;
	RecordId recordId = null;
	
	public TableRecord(Map<Integer, DBType> map) {
		columnMap = map;
	}
	
	public RecordId getRecordId() {
		return recordId;
	}

	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}

	public Map<Integer, DBType> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(Map<Integer, DBType> columnMap) {
		this.columnMap = columnMap;
	}
	
	public void loadExtendedColumns(List<Integer> columns, TypeMetadata meta, Set<Object> pinnedBlocks) {
		TableRecordMetadata trsm = (TableRecordMetadata) meta;
		Iterator<Integer> iter = columns.iterator();
		while (iter.hasNext()) {
			int columnId = iter.next();
			DBType column = columnMap.get(columnId);
			if (column instanceof ExtendedColumn && ((ExtendedColumn) column).getValue() == null) {
				ColumnSerializer.getInstance().readFull(column, new ColumnSerializerMetadata(trsm.getColumnIdTypeMap().get(columnId)), pinnedBlocks);
			}
		}
	}
	
	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getResourceCount() {
		int size = 0;
		Iterator<DBType> iter = columnMap.values().iterator();
		while (iter.hasNext()) {
			DBType column = iter.next();
			if (column != null && column instanceof ExtendedColumn) {
				size = size + ((Extended) column).getPtrList().size();
			}
		}
		return size;
	}

}
