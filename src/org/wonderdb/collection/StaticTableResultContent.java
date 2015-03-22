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

import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.record.TableRecord;



public class StaticTableResultContent implements ResultContent {
	TableRecord tr = null;
	
	public StaticTableResultContent(TableRecord tr) {
		this.tr = tr;
	}
	
	@Override
	public DBType getValue(Integer ct) {
		DBType column = tr.getColumnMap().get(ct);
		if (column != null) {
			if (column instanceof ExtendedColumn) {
				return ((ExtendedColumn) column).getValue(null);
			}
			return column;
		}
		return null;
	}

	@Override
	public Map<Integer, DBType> getAllColumns() {
		Map<Integer, DBType> retMap = new HashMap<Integer, DBType>();
		Iterator<Integer> iter = tr.getColumnMap().keySet().iterator();
		while (iter.hasNext()) {
			int key = iter.next();
			DBType val = tr.getColumnMap().get(key);
			retMap.put(key, val);
		}
		return retMap;
	}

	@Override
	public RecordId getRecordId() {
		return tr.getRecordId();
	}

	public TableRecord getTableRecord() {
		return tr;
	}
}
