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
package org.wonderdb.block.record;
import java.util.List;
import java.util.Map;

import org.wonderdb.block.index.impl.base.Queriable;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.DBType;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;



public interface QueriableBlockRecord extends Queriable, Cacheable, SerializableType {
	Map<ColumnType, DBType> getColumns();
	Map<ColumnType, DBType> copyAndGetColumns(List<ColumnType> selectColums);
	void setColumns(Map<ColumnType, DBType> colValues);
//	DBType getColumnValue(ColumnType name, int schemaId);
	void setColumnValue(ColumnType name, DBType value);
	void removeColumn(ColumnType name);
	long getLastAccessDate();
	long getLastModifiedDate();
	void updateLastAccessDate();
	void updateLastModifiedDate();
	RecordId getRecordId();
	void setRecordId(RecordId recordId);
	int getBufferCount();
	void setBufferCount(int count);
}
