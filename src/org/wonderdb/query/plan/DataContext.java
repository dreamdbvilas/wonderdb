package org.wonderdb.query.plan;

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

import org.wonderdb.collection.ResultContent;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.schema.WonderDBFunction;
import org.wonderdb.types.DBType;
import org.wonderdb.types.RecordId;



public class DataContext {
	Map<Integer, DBType> globalContext = new HashMap<Integer, DBType>();
	Map<CollectionAlias, ResultContent> map = new HashMap<CollectionAlias, ResultContent>(); 

	public DataContext() {
	}
	
	public DataContext(DataContext context) {
		// copy only global context
//		map = new HashMap<CollectionAlias, ResultContent>(context.map);
		globalContext = context.globalContext;
	}
	
	public void add(CollectionAlias ca, ResultContent trt) {
		map.put(ca, trt);
	}
	
	public void remove(CollectionAlias ca) {
		map.remove(ca);
	}
	
//	public Map<Integer, DBType> getAllColumns(CollectionAlias ca) {
//		ResultContent resultContent = map.get(ca);
//		return resultContent.getAllColumns(); 
//	}
	
	public DBType getGlobalValue(Integer ct) {
		return globalContext.get(ct);
	}
	
	public void processFunction(WonderDBFunction fn) {
		DBType dt = fn.process(this);
		globalContext.put(fn.getColumnType(), dt);
	}
	
	public DBType getValue(CollectionAlias ca, Integer columnName, String path) {
//		Queriable trt = map.get(ca);
//		if (trt == null) {
//			return null;
//		}
//		return trt.getColumnValue(columnName);
		ResultContent rc = map.get(ca);
		if (rc != null) {
			return rc.getValue(columnName);
		}
		return null;
	}
	
	public DBType getValue(CollectionAlias ca, String columnName, String path) {
		CollectionMetadata colMetadata = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
		Integer ct = colMetadata.getColumnId(columnName);
		return getValue(ca, ct, path);
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
			b.append("Collection Name = "+ca.getCollectionName()+"\n");
//			RecordId trt = map.get(ca);
//			trt.toString();
		}
		return b.toString();
	}
}
