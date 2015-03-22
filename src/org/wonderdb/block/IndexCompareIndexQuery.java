package org.wonderdb.block;

import java.util.Comparator;
import java.util.Set;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;

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


public class IndexCompareIndexQuery implements IndexQuery, Comparator<DBType> {
	DBType key;
	Set<Object> pinnedBlocks = null;
	TypeMetadata meta = null;
	boolean includeKey = false;
	
	public IndexCompareIndexQuery(DBType key, boolean includeKey, TypeMetadata meta, Set<Object> pinnedBlocks) {
		this.key = key;
		this.pinnedBlocks = pinnedBlocks;
		this.meta = meta;
		this.includeKey = includeKey;
	}
	
	public int compareTo(DBType k) {
		return this.key.compareTo(k);
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Comparator<DBType> getComparator() {
		return this;
	}

	@Override
	public int compare(DBType o1, DBType o2) {
		DBType column = null;
		if (o1 instanceof IndexRecord) {
			column = ((IndexRecord) o1).getColumn();
			if (column instanceof BlockPtr) {
				IndexBlock block = (IndexBlock) BlockManager.getInstance().getBlock((BlockPtr) column, 
						meta, pinnedBlocks);
				column = block.getMaxKey(meta);
			} 
			
			if (column instanceof ExtendedColumn) {
				column = ((ExtendedColumn) column).getValue(null);
			}
			
			IndexKeyType ikt = (IndexKeyType) ((IndexCompareIndexQuery) o2).key;
			int val = ikt.compareTo(column);
			val = val * -1;
//				int val = column.getValue().compareTo(((IndexCompareIndexQuery) o2).key);
			if (val == 0) {
				if (includeKey) {
					return 1;
				} else {
					return -1;
				}
			}
			return val;
		}
		return -1;
	}
}
