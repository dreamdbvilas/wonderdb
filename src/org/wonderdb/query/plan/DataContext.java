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
package org.wonderdb.query.plan;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;


public class DataContext {
	Map<CollectionAlias, ResultContent> map = new HashMap<CollectionAlias, ResultContent>(); 

	public DataContext() {
	}
	
	public DataContext(DataContext context) {
		map = new HashMap<CollectionAlias, ResultContent>(context.map);
	}
	
	public void add(CollectionAlias ca, ResultContent trt) {
		map.put(ca, trt);
	}
	
	public void remove(CollectionAlias ca) {
		map.remove(ca);
	}
	
	public Map<ColumnType, DBType> getAllColumns(CollectionAlias ca) {
		ResultContent resultContent = map.get(ca);
		return resultContent.getAllColumns(); 
	}
	
	public DBType getValue(CollectionAlias ca, ColumnType columnName, String path) {
//		Queriable trt = map.get(ca);
//		if (trt == null) {
//			return null;
//		}
//		return trt.getColumnValue(columnName);
		ResultContent rc = map.get(ca);
		if (rc != null) {
			return rc.getValue(columnName, path);
		}
		return null;
	}
	
	public RecordId get(CollectionAlias ca) {
		if (map != null && map.get(ca) != null) {
			return map.get(ca).getRecordId();
		}
		return null;
	}
	
	public Map<CollectionAlias, ResultContent> getResultMap() {
		return map;
	}
	
	public String toString() {
		Iterator<CollectionAlias> iter = map.keySet().iterator();
		StringBuilder b = new StringBuilder();
		
		while (iter.hasNext()) {
			CollectionAlias ca = iter.next();
			b.append("Collection Name = "+ca.collectionName+"\n");
//			RecordId trt = map.get(ca);
//			trt.toString();
		}
		return b.toString();
	}
}
