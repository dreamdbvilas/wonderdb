package org.wonderdb.cluster;

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

import org.wonderdb.types.DBType;


public class Shard {
	String indexName;
	String replicaSetName;

	DBType min = null;
	DBType max = null;
	
	public Shard(String indexName) {
		if (indexName == null) {
			throw new RuntimeException("Invalid input: ");
		}
		this.indexName = indexName;
	}
	
	public void setMinMap(DBType min) {
		if (min == null) {
			return;
		}
		this.min = min;
	}
	
	public void setMaxMap(DBType max) {
		if (max == null) {
			return;
		}
		this.max = max;
	}
	
	public DBType getMin() {
		return min;
	}
	
	public DBType getMax() {
		return max;
	}
	
	@Override
	public int hashCode() {
		return indexName.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (o instanceof Shard) {
			Shard tmp = (Shard) o;
			return this.indexName.equals(tmp.indexName) && this.max.equals(tmp.max) && this.min.equals(tmp.min);
		}
		
		return false;
	}
}
