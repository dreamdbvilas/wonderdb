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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.wonderdb.collection.exceptions.InvalidIndexException;
import org.wonderdb.types.impl.ColumnType;


public class Index {
	private String indexName;
	private String collectionName;
	private List<CollectionColumn> columnList;
	private boolean unique = false;
	private boolean asc = true;
	
	public Index(String indexName, String collectionName, List<CollectionColumn> columnList) 
		throws InvalidIndexException {
		this(indexName, collectionName, columnList, false, true);
	}
	
	public Index(String indexName, String collectionName, 
			List<CollectionColumn> columnList, boolean unique, boolean asc) 
		throws InvalidIndexException {
		if (columnList == null || collectionName == null || indexName == null || columnList.size() == 0) {
			throw new InvalidIndexException("Invalid Index: indexName = " + indexName + " Collection name = " + collectionName + " Column List = " + columnList);
		}
		
		this.indexName = indexName;
		this.collectionName = collectionName;
		this.columnList = columnList;
		this.unique = unique;
		this.asc = asc;
	}
	
	public void setColumnList(List<CollectionColumn> list) {
		this.columnList = list;
	}
	
	public boolean getAsc() {
		return asc;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public List<CollectionColumn> getColumnList() {
		return this.columnList;
	}
	
	public Set<ColumnType> getColumnNameList() {
		Set<ColumnType> list = new HashSet<ColumnType>();
		for(CollectionColumn c : columnList) {
			list.add(c.getColumnType());
		}
		return list;
	}
	
	public boolean isUnique() {
		return unique;
	}
	
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		Index i = null;
		if (o instanceof Index) {
			i = (Index) o;
		}
		
		if (i == null) {
			return false;
		}
		
		if (!this.getIndexName().equals(i.getIndexName()) ||
				!this.collectionName.equals(i.collectionName)) {
			return false;
		}
		
		if (this.columnList.size() != i.columnList.size()) {
			return false;
		}
		
		for (int x = 0; x < columnList.size(); x++) {
			if (!columnList.get(x).equals(i.columnList.get(x))) {
				return false;
			}
		}
		return true;
	}
	
	public int hashCode() {
		return getIndexName().hashCode();
	}	
}
