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
package org.wonderdb.seralizers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.RecordHeaderType;
import org.wonderdb.types.impl.RecordValuesType;


public class ArrangeColumnsHelper {
	// singleton
	private static final ArrangeColumnsHelper instance = new ArrangeColumnsHelper();
	private static final ValueSizeComparator vInstance = instance.new ValueSizeComparator();
	
	// singleton
	private ArrangeColumnsHelper() {
	}
	
	// singleton
	public static ArrangeColumnsHelper getInstance() {
		return instance;
	}
	
	public void arrange(Map<ColumnType, DBType> changedColumns, int maxSize, List<List<ColumnType>> columnsByBlockList, 
			List<List<DBType>> valueByBlockList) {
		Collection<DBType> valueCollection = changedColumns.values();
		List<DBType> valueList = new LinkedList<DBType>(valueCollection);
		Collections.sort(valueList, vInstance);
		List<DBType> currentBlockValues = new ArrayList<DBType>();
		int currentBlockSize = 0;
		valueByBlockList.add(currentBlockValues);
		
		while (valueList.size() > 0) {
			DBType value = valueList.remove(0);
				if (currentBlockValues.isEmpty()) {
					currentBlockValues.add(value);
					currentBlockSize = value.getByteSize();
				} else if (value.getByteSize() + currentBlockSize > maxSize) {
					currentBlockValues = new ArrayList<DBType>();
					valueByBlockList.add(currentBlockValues);
					currentBlockValues.add(value);
					currentBlockSize = value.getByteSize();
				} else {
					currentBlockValues.add(value);
					currentBlockSize = currentBlockSize + value.getByteSize();
			}
		}
		
		Map<DBType, ColumnType> reverseMap = new HashMap<DBType, ColumnType>();
		Iterator<ColumnType> iter = changedColumns.keySet().iterator();
		while (iter.hasNext()) {
			ColumnType ct = iter.next();
			DBType value = changedColumns.get(ct);
			reverseMap.put(value, ct);
		}
		
		List<ColumnType> currentBlockColumns = null;
		
		for (int i = 0; i < valueByBlockList.size(); i++) {
			List<DBType> valuesInBlock = valueByBlockList.get(i);
			currentBlockColumns = new ArrayList<ColumnType>();
			columnsByBlockList.add(currentBlockColumns);
			for (int x = 0; x < valuesInBlock.size(); x++) {
				DBType dt = valuesInBlock.get(x);
				currentBlockColumns.add(reverseMap.get(dt));
			}
		}		
	}
	
	public void arrange(Map<ColumnType, DBType> changedColumns, int maxSize, RecordHeaderType rht, 
			RecordValuesType rvt) {
		
	}	
	
	
	public class ValueSizeComparator implements Comparator<DBType> {
		public int compare(DBType l, DBType r) {
			int lSize = l.getByteSize();
			int rSize = r.getByteSize();
			if (lSize > rSize) {
				return 1;
			} else if (lSize < rSize) {
				return -1;
			}
			return 0;
		}
	}
}
