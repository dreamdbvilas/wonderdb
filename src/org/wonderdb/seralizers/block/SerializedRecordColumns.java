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
package org.wonderdb.seralizers.block;

import java.util.List;

import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.types.SerializableType;
import org.wonderdb.types.impl.ColumnType;


public interface SerializedRecordColumns {

	public abstract RecordId getRecordId();

	public abstract int getMaxContiguousFreeSize();
	
	public void update(List<ColumnType> columns, List<SerializableType> values);

	public SerializableType getValue(ColumnType columnType);
	
	public List<ColumnType> getColumns();
	
	public List<SerializableType> getValues();
	
	public int getTotalFreeSize();
}