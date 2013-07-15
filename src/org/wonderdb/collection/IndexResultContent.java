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
package org.wonderdb.collection;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;


public class IndexResultContent implements ResultContent {
	IndexKeyType ikt = null;
	int schemaId = -1;
	
	public IndexResultContent(IndexKeyType ikt, int schemaId) {
		this.ikt = ikt;
		this.schemaId = schemaId;
	}
	
	@Override
	public DBType getValue(ColumnType ct, String path) {
		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex(schemaId);
		List<CollectionColumn> list = idxMeta.getIndex().getColumnList();
		CollectionColumn cc = new CollectionColumn(null, (Integer) ct.getValue(), null, true, false);
		int i = Collections.binarySearch(list, cc);
		if (i >= 0) {
			return ikt.getValue().get(i);
		}
		throw new ValueNotAvailableException();
	}
	
	@Override
	public Map<ColumnType, DBType> getAllColumns() {
		throw new RuntimeException("Method not found");
	}

	
	@Override
	public RecordId getRecordId() {
		return ikt.getRecordId();
	}

	@Override
	public int getSchemaId() {
		return schemaId;
	}

	@Override
	public int getCollectionSchemaId() {
		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex(schemaId);
		return SchemaMetadata.getInstance().getCollectionMetadata(idxMeta.getIndex().getCollectionName()).getSchemaId();
	}
}
