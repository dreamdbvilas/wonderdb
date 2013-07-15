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
package org.wonderdb.block.record.table;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.record.impl.base.BaseRecord;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;


public class TableRecord extends BaseRecord {
	
	public TableRecord(Map<ColumnType, DBType> map) {
		this(null, map);
	}
	
	public TableRecord(RecordId recordId, Map<ColumnType, DBType> map) {
		super(recordId);
		if (map != null) {
			super.setColumns(map);
		}
	}
	
	public String getSerializerName() {
		return "TS";
	}
	
	public void addColumn(ColumnType columnType, DBType value) {
		super.getColumns().put(columnType, value);
	}
		
//	public DBType getColumnValue(String cName, int schemaId) {
//		return super.getColumnValue(new ColumnType(cName), schemaId);
//	}
	
//	public int getByteSize() {
//		return TableRecordSerializer.getInstance().getByteSize(this);
//	}	
//	
	@SuppressWarnings("unchecked")
	public void updateContents(Object o) {
		Map<ColumnType, DBType> map = null;
		if (o instanceof Map) {
			map = (Map<ColumnType, DBType>) o;
		} else {
			throw new RuntimeException("Invalid type: " + 0);
		}
		
		super.getColumns().putAll(map);
	}
	
	@SuppressWarnings("unchecked")
	public void removeContents(Object o) {
		Set<String> set = null;
		if (o instanceof Set) {
			set = (Set<String>) o;
		} else {
			throw new RuntimeException("Invalid type: " + o);
		}
		
		Iterator<String> iter = set.iterator();
		while (iter.hasNext()) {
			String name = iter.next();
			ColumnType ct = new ColumnType(name);
			super.getColumns().remove(ct);
		}
	}
}
