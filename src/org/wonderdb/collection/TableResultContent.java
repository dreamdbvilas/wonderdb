package org.wonderdb.collection;

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.TableRecord;



public class TableResultContent extends TableRecord implements ResultContent {
	private TableRecord record = null;
	private TypeMetadata meta = null;
	private Set<Object> pinnedBlocks = null;
	
	public TableResultContent(TableRecord record, TypeMetadata meta, Set<Object> pinnedBlocks) {	
		super(record.getColumnMap());
		this.record = record;
		this.meta = meta;
		this.pinnedBlocks = pinnedBlocks;
	}
	
	@Override
	public DBType getValue(Integer ct) {
		if (record == null) {
			return null;
		}

		DBType column = record.getColumnMap().get(ct);
		if (column == null) {
			return null;
		}
		
		if (column instanceof Extended && ((ExtendedColumn)column).getValue() == null) {
			ColumnSerializer.getInstance().readFull(column, meta, pinnedBlocks);
			record.getColumnMap().put(ct, column);
			return ((ExtendedColumn) column).getValue();
		}
		return column;
	}

	@Override
	public Map<Integer, DBType> getAllColumns() {
		Map<Integer, DBType> retMap = new HashMap<Integer, DBType>();
		Map<Integer, DBType> map = record.getColumnMap();
		Iterator<Integer> iter = map.keySet().iterator();
		while (iter.hasNext()) {
			int key = iter.next();
			DBType dbtype = getValue(key);
			retMap.put(key, dbtype);
		}
		return retMap;
	}

	@Override
	public RecordId getRecordId() {
		return record.getRecordId();
	}
}
