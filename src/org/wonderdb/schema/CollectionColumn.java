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
package org.wonderdb.schema;

import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;

public class CollectionColumn implements SerializableType, DBType {
	public static final int BASE_SIZE = Integer.SIZE/8 + Integer.SIZE/8 + Integer.SIZE/8 + 2;
	
	String columnName;
	int columnId = -1;
	String serializerName;
	boolean isNullable = false;
	boolean isQueriable = true;
	
	public CollectionColumn(CollectionMetadata colMeta, String columnName, String serializerName, boolean isNullable, boolean isQueriable) {
		this(columnName, colMeta.getColumnId(columnName), serializerName, isNullable, isQueriable);
	}	

	public CollectionColumn(CollectionMetadata colMeta, String columnName, String serializerName, boolean isNullable) {
		this(columnName, colMeta.getColumnId(columnName), serializerName, isNullable, true);
	}	

	public CollectionColumn(CollectionMetadata colMeta, String columnName, String serializerName) {
		this(colMeta, columnName, serializerName, true, true);
	}
	
	public CollectionColumn(String columnName, int id, String serializerName, boolean isNullable, boolean isQueriable) {
		this.columnName = columnName;
		this.columnId = id;
		this.serializerName = serializerName;
		this.isNullable = isNullable;
		this.isQueriable = isQueriable;
	}
	
	public ColumnType getColumnType() {
		if (columnId < 0) {
			return new ColumnType(columnName);
		}
		return new ColumnType(columnId);
	}
	
	public String getSerializerName() {
//		return serializerName;
		return "cc";
	}
	
	public String getCollectionColumnSerializerName() {
		return serializerName;
	}
	
	public boolean isNullable() {
		return isNullable;
	}
	
	public boolean isQueriable() {
		return isQueriable;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public int getColumnId() {
		return columnId;
	}
	
	public void setColumnId(int id) {
		columnId = id;
	}
	
	public boolean equals(Object o) {
		CollectionColumn i = null;
		if (o instanceof CollectionColumn) {
			i = (CollectionColumn) o;
		}
		
		if (i == null) {
			return false;
		}
		
		return this.columnName.equals(i.columnName) || this.columnId == i.columnId;
	}
	
	@Override
	public int getByteSize() {
		return BASE_SIZE + columnName.getBytes().length + serializerName.getBytes().length;
	}

	@Override
	public int compareTo(DBType arg0) {
		CollectionColumn cc = null;
		if (arg0 instanceof CollectionColumn) {
			cc = (CollectionColumn) arg0;
		} else {
			return -1;
		}
		
		if (this.getColumnId() > cc.getColumnId()) {
			return 1;
		} else if (this.getColumnId() < cc.getColumnId()) {
			return -1;
		}
		return 0;
	}

	@Override
	public DBType copyOf() {
		return new CollectionColumn(this.columnName, this.columnId, this.serializerName, this.isNullable, this.isQueriable);
	}
}
