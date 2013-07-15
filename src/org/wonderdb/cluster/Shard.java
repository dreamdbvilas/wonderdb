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
package org.wonderdb.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;

public class Shard {
	int schemaId = -1;
	String schemaObjectName;
	String replicaSetName;
//	String masterMachineId;
//	IndexKeyType ikt;
	Map<ColumnType, DBType> minMap = null;
	Map<ColumnType, DBType> maxMap = null;
	
	public Shard(int schemaId, String collectionName, String replicaSet) {
		if (collectionName == null || replicaSet == null) {
			throw new RuntimeException("Invalid input: ");
		}
		this.schemaId = schemaId;
		this.schemaObjectName = collectionName;
		this.replicaSetName = replicaSet;
	}
	
	public int getSchemaId() {
		return schemaId;
	}
	
	public String getSchemaObjectName() {
		return schemaObjectName;
	}

	public String getReplicaSetName() {
		return replicaSetName;
	}
	
//	public String getMasterMachineId() {
//		return masterMachineId;
//	}
//
//	public void setMasterMachineId(String masterMachineId) {
//		this.masterMachineId = masterMachineId;
//	}
//	
//	public IndexKeyType getIkt() {
//		return ikt;
//	}
	
	public void setMinMap(IndexKeyType ikt) {
		if (ikt == null) {
			return;
		}
		List<CollectionColumn> list = SchemaMetadata.getInstance().getIndex(schemaId).getIndex().getColumnList();
		Map<ColumnType, DBType> map = new HashMap<ColumnType, DBType>();
		for (int i = 0; i < list.size(); i++) {
			ColumnType ct = list.get(i).getColumnType();
			map.put(ct, ikt.getValue().get(i));
		}
		
		minMap = map;
	}
	
	public void setMaxMap(IndexKeyType ikt) {
		if (ikt == null) {
			return;
		}
		List<CollectionColumn> list = SchemaMetadata.getInstance().getIndex(schemaId).getIndex().getColumnList();
		Map<ColumnType, DBType> map = new HashMap<ColumnType, DBType>();
		for (int i = 0; i < list.size(); i++) {
			ColumnType ct = list.get(i).getColumnType();
			map.put(ct, ikt.getValue().get(i));
		}
		
		maxMap = map;
	}
	
	public Map<ColumnType, DBType> getMinMap() {
		return minMap;
	}
	
	public Map<ColumnType, DBType> getMaxMap() {
		return maxMap;
	}
	
	@Override
	public int hashCode() {
		return schemaObjectName.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (o instanceof Shard) {
			Shard tmp = (Shard) o;
			return (tmp.schemaId == this.schemaId && this.schemaId < 3) ||
					tmp.schemaObjectName.equals(this.schemaObjectName) && tmp.replicaSetName.equals(this.replicaSetName);
		}
		
		return false;
	}
}
