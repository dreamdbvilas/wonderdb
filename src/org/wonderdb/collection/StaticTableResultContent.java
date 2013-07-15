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

import java.util.Map;

import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;


public class StaticTableResultContent implements ResultContent {
	TableRecord tr = null;
	int schemaId = -1;
	
	public StaticTableResultContent(TableRecord tr, RecordId recId, int schemaId) {
		this.tr = tr;
		this.schemaId = schemaId;
		if (this.tr != null) {
			this.tr.setRecordId(recId);
		}
	}
	
	@Override
	public DBType getValue(ColumnType ct, String path) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
		return tr.getColumnValue(colMeta, ct, path);
	}

	@Override
	public Map<ColumnType, DBType> getAllColumns() {
		return tr.getColumns();
	}

	@Override
	public RecordId getRecordId() {
		return tr.getRecordId();
	}

	@Override
	public int getSchemaId() {
		return schemaId;
	}
	
	public TableRecord getTableRecord() {
		return tr;
	}

	@Override
	public int getCollectionSchemaId() {
		return schemaId;
	}

}
