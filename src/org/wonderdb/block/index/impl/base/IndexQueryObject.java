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
package org.wonderdb.block.index.impl.base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.Index;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;


public class IndexQueryObject implements Queriable {
	Map<ColumnType, DBType> map = new HashMap<ColumnType, DBType>();
	
	public IndexQueryObject(Index idx, IndexKeyType ikt) {
		List<CollectionColumn> list = idx.getColumnList();
		for (int i = 0; i < list.size(); i++) {
			ColumnType s = list.get(i).getColumnType();
			DBType val = ikt.getValue().get(i);
			map.put(s, val);
		}
	}
	
	public DBType getColumnValue(CollectionMetadata colMeta, ColumnType ct, String path) {
		return map.get(ct);
	}
	
	public Map<ColumnType, DBType> getQueriableColumns() {
		return map;
	}
	
	public long getMemoryFootprint() {
		throw new RuntimeException("Method not supported");
	}
}
