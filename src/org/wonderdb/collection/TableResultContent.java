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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.wonderdb.block.record.QueriableBlockRecord;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;


public class TableResultContent implements ResultContent {
	private RecordId recordId = null;
	private int schemaId = -1;
	private RecordBlock lockedBlock = null;
	
	public TableResultContent(RecordBlock lockedBlock, RecordId recordId, int schemaId) {
		this.recordId = recordId;
		this.schemaId = schemaId;
		this.lockedBlock = lockedBlock;
	}
	
	@Override
	public DBType getValue(ColumnType ct, String path) {
		if (recordId == null) {
			return null;
		}
		
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId); 
		int schemaId = colMeta.getSchemaId();
		List<ColumnType> ctList = new ArrayList<ColumnType>();

		if (ct.getValue() instanceof String) {
			ct = colMeta.getColumnType((String) ct.getValue());
		}
		if (ct != null && colMeta.getSchemaId() >= 3) {
			ctList.add(ct);
			QueriableBlockRecord record = CacheObjectMgr.getInstance().getRecord(lockedBlock, recordId, ctList, schemaId, null);
			if (record != null) {
				return record.getColumnValue(colMeta, ct, path);
			}
		}
		return null;
	}

	@Override
	public Map<ColumnType, DBType> getAllColumns() {
		return CacheObjectMgr.getInstance().getRecord(lockedBlock, recordId, null, schemaId, null).getColumns(); 
	}

	@Override
	public RecordId getRecordId() {
		return recordId;
	}

	@Override
	public int getSchemaId() {
		return schemaId;
	}

	@Override
	public int getCollectionSchemaId() {
		return schemaId;
	}

}
