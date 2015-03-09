package org.wonderdb.query.parse;

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



public class CollectionAlias {
	String collectionName;
	String alias = "";
	
	public CollectionAlias(String collectionName, String alias) {
		this.collectionName = collectionName;
		if (alias != null) {
			this.alias = alias;
		}
	}
	
	public CollectionAlias(CollectionAlias ca) {
		collectionName = ca.collectionName;
		alias = ca.alias;
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public int hashCode() {
		return (collectionName+"_"+alias).hashCode();
	}
	
	public boolean equals(Object o) {
		CollectionAlias ca = null;
		if (o instanceof CollectionAlias) {
			ca = (CollectionAlias) o;
		}
		
		if (ca == null) {
			return false;
		}
		
		return collectionName.equals(ca.collectionName) && alias.equals(ca.alias);
	}	
}
