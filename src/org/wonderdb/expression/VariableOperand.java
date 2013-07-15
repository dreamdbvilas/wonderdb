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
package org.wonderdb.expression;

import java.util.List;

import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;


public class VariableOperand implements Operand {
	CollectionAlias collectionAlias;
	ColumnType columnType;
	String expression = null;
	
	public VariableOperand(CollectionAlias collectionAlias, ColumnType columnType, String expression) {
		this.collectionAlias = collectionAlias;
		this.columnType = columnType;
		this.expression = expression;
	}
	
	public CollectionAlias getCollectionAlias() {
		return collectionAlias;
	}

	public ColumnType getColumnType() {
		return columnType;
	}
	
	public Comparable<DBType> getValue(Index idx, IndexKeyType key) {
		List<DBType> val = key.getValue();
		if (val == null || val.size() == 0) {
			return null;
		}
		List<CollectionColumn> list = idx.getColumnList();
		for (int i = 0; i < list.size(); i++) {
			ColumnType colType = list.get(i).getColumnType();
			if (colType.equals(columnType)) {
				return val.get(i);
			}
		}
		return null;
	}
	
	public Comparable<DBType> getValue(TableRecord trt) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionAlias.getCollectionName());
		return trt.getColumnValue(colMeta, columnType, expression);
	}
	
	public String getPath() {
		return expression;
	}
	
	public boolean equals(Object o) {
		VariableOperand op = null;
		if (o instanceof VariableOperand) {
			op = (VariableOperand) o;
		}
		
		if (op == null) {
			return false;
		}
		
		return collectionAlias.equals(op.collectionAlias) && columnType.equals(op.columnType);
	}
}
