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
import java.util.List;
import java.util.Map;

import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexRecordMetadata;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.UndefinedType;
import org.wonderdb.types.record.IndexRecord;



public class IndexResultContent extends IndexRecord implements ResultContent {
	IndexKeyType ikt = null;
	IndexRecordMetadata meta = null;
	
	public IndexResultContent(IndexKeyType ikt, TypeMetadata meta) {
		this.ikt = ikt;
		this.meta = (IndexRecordMetadata) meta;
	}
	
	@Override
	public DBType getValue(Integer ct) {
		List<Integer> list = meta.getColumnIdList();
		int posn = list.indexOf(ct);
		if (posn >= 0) {
			DBType dt = ikt.getValue().get(posn);
			if (dt instanceof ExtendedColumn) {
				return ((ExtendedColumn) dt).getValue();
			}
		}
		return UndefinedType.getInstance();
	}
	
	@Override
	public Map<Integer, DBType> getAllColumns() {
		Map<Integer, DBType> map = new HashMap<>();
		for (int i = 0; i < meta.getColumnIdList().size(); i++) {
			map.put(meta.getColumnIdList().get(i), ikt.getValue().get(i));
		}
		return map;
	}

	
	@Override
	public RecordId getRecordId() {
		return ikt.getRecordId();
	}
}
