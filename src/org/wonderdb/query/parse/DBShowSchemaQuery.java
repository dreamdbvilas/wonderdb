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
package org.wonderdb.query.parse;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.wonderdb.expression.AndExpression;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;


public class DBShowSchemaQuery extends BaseDBQuery {
	private static DBShowSchemaQuery instance = new DBShowSchemaQuery();
	private DBShowSchemaQuery() {
		super(null, -1, null);
	}
	
	public static DBShowSchemaQuery getInstance() {
		return instance;
	}
	
	public void execute(String query, ByteBuffer buffer) {
		SchemaMetadata meta = SchemaMetadata.getInstance();
		Map<String, CollectionMetadata> collections = meta.getCollections();
		Iterator<String> iter = collections.keySet().iterator();
		while (iter.hasNext()) {
			String collectionName = iter.next();
			CollectionMetadata cMeta = collections.get(collectionName);
			buffer.put(("CollectionName = " + cMeta.getName()+"\n").getBytes());
//			buffer.put(("Data File = " + cMeta.getCollectionFileName()+"\n").getBytes());
			buffer.put(("Object Count = " + cMeta.getCount()+"\n").getBytes());
//			buffer.put(("Byte Size = " + cMeta.getSize()+"\n").getBytes());
			buffer.put(("Serializer Name = " + cMeta.getSerialierName()+"\n").getBytes());

			List<IndexMetadata> idxs = meta.getIndexes(collectionName);
			if (idxs != null && idxs.size() > 0) {
				buffer.put("    Indexes:\n".getBytes());
				for (int i = 0; i < idxs.size(); i++) {
					IndexMetadata idx = idxs.get(i);
					buffer.put(("    Index Name = "+idx.getName()+"\n").getBytes());
					buffer.put(("    Unique = "+idx.getIndex().isUnique()+"\n").getBytes());
					buffer.put(("    Ascending = "+idx.getIndex().getAsc()+"\n").getBytes());
					List<CollectionColumn> list = idx.getIndex().getColumnList();
					for (int x = 0; x < list.size(); x++) {
						CollectionColumn c = list.get(x);
						buffer.put(("        Column Name = "+c.getColumnName()+"\n").getBytes());
						buffer.put(("        Serializer Name = "+c.getSerializerName()+"\n").getBytes());
					}
					buffer.put(("    Index Data File Name = \n").getBytes());
//					buffer.put(("    Index Byte Size = "+idx.getSize()+"\n").getBytes());
					buffer.put(("    Serializer Name = "+idx.getSerialierName()+"\n").getBytes());
				}				
			}
		}
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
