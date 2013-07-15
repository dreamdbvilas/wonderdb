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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.RecordHeaderType;


public class RecordHeaderTypeSerializer {
												   
	private static RecordHeaderTypeSerializer instance = new RecordHeaderTypeSerializer();
	
	private RecordHeaderTypeSerializer() {
	}
	
	public static RecordHeaderTypeSerializer getInstance() {
		return instance;
	}
	
	public void toBytes(RecordHeaderType rht, ChannelBuffer buffer) {

		buffer.clear();
		buffer.writeInt(rht.getHeader().size());
		
		Iterator<RecordId> iter = rht.getHeader().keySet().iterator();
		while (iter.hasNext()) {
			RecordId recordId = iter.next();
			BlockPtrSerializer.getInstance().toBytes(recordId.getPtr(), buffer);
			buffer.writeInt(recordId.getPosn());
			Collection<ColumnType> columnList = rht.getHeader().get(recordId);
			buffer.writeInt(columnList.size());
			Iterator<ColumnType> ctIter = columnList.iterator();
			while (ctIter.hasNext()) {
				ColumnType ct = ctIter.next();
				ColumnTypeSerializer.getInstance().toByte(ct, buffer);
			}
		}		
	}
	
	public RecordHeaderType unmarshal(ChannelBuffer buffer) {
		int mapSize = buffer.readInt();
		Map<RecordId, List<ColumnType>> map = new HashMap<RecordId, List<ColumnType>>();
		for (int i = 0; i < mapSize; i++) {
			BlockPtr ptr = BlockPtrSerializer.getInstance().unmarshal(buffer);
			int posn = buffer.readInt();
			RecordId recordId = new RecordId(ptr, posn);
			int listSize = buffer.readInt();
			List<ColumnType> set = new ArrayList<ColumnType>();
			map.put(recordId, set);
			for (int j = 0; j < listSize; j++) {
				ColumnType ct = ColumnTypeSerializer.getInstance().unmarshal(buffer);
				set.add(ct);
			}
		}
		RecordHeaderType rht = new RecordHeaderType();
		rht.setHeaderMap(map);
		return rht;
	}	
}
