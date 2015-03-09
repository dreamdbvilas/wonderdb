package org.wonderdb.expression;

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

import java.util.List;
import java.util.Set;

import org.wonderdb.collection.TableResultContent;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.TableRecord;



public class VariableOperand implements Operand {
	CollectionAlias collectionAlias;
	Integer columnId;
	String path = null;
	
	public VariableOperand(CollectionAlias collectionAlias, Integer columnId, String path) {
		this.collectionAlias = collectionAlias;
		this.columnId = columnId;
		this.path = path;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public CollectionAlias getCollectionAlias() {
		return collectionAlias;
	}

	public Integer getColumnId() {
		return columnId;
	}
	
	public DBType getValue(IndexKeyType key, TypeMetadata meta) {
		IndexRecordMetadata ism = (IndexRecordMetadata) meta;
		List<DBType> val = key.getValue();
		if (val == null || val.size() == 0) {
			return null;
		}
		
		List<Integer> list = ism.getTypeList();
		for (int i = 0; i < list.size(); i++) {
			Integer colType = list.get(i);
			if (colType.equals(columnId)) {
				return val.get(i);
			}
		}
		return null;
	}
	
	@Override
	public DBType getValue(TableRecord trt, TypeMetadata meta, Set<Object> pinnedBlocks) {
		TableResultContent trc = new TableResultContent(trt, meta, pinnedBlocks);
		return trc.getValue(columnId);
	}
	
	public boolean equals(Object o) {
		VariableOperand op = null;
		if (o instanceof VariableOperand) {
			op = (VariableOperand) o;
		}
		
		if (op == null) {
			return false;
		}
		
		return collectionAlias.equals(op.collectionAlias) && columnId.equals(op.columnId);
	}
}
