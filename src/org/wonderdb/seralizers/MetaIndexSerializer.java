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
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.collection.exceptions.InvalidIndexException;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.impl.StringType;


public class MetaIndexSerializer {
	private static MetaIndexSerializer instance = new MetaIndexSerializer();
	
	private MetaIndexSerializer() {
	}
	
	public static MetaIndexSerializer getInstance() {
		return instance;
	}
	
	public void toBytes(IndexMetadata index, ChannelBuffer buffer) {
//		buffer.writeByte(index.getFileId());
//		StringSerializer.getInstance().toBytes(new StringType(index.getFileName()), buffer);
		StringSerializer.getInstance().toBytes(new StringType(index.getName()), buffer);
		buffer.writeInt(index.getSchemaId());
		buffer.writeByte(index.getIndex().getAsc() == true ? (byte) 1 : (byte) 0);
		buffer.writeByte(index.getIndex().isUnique() == true ? (byte) 1 : (byte) 0);
		StringSerializer.getInstance().toBytes(new StringType(index.getIndex().getCollectionName()), buffer);
		List<CollectionColumn> list = index.getIndex().getColumnList();
		buffer.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			buffer.writeInt(list.get(i).getColumnId());
		}
	}

	public IndexMetadata unmarshal(ChannelBuffer buffer) {
//		byte fileId = buffer.readByte();
//		String fileName = StringSerializer.getInstance().unmarshal(buffer).get();
		String indexName = StringSerializer.getInstance().unmarshal(buffer).get();
		int schemaId = buffer.readInt();
		boolean asc = buffer.readByte() == 1 ? true : false;
		boolean unique = buffer.readByte() == 1 ? true : false;
		String collectionName = StringSerializer.getInstance().unmarshal(buffer).get();
		int count = buffer.readInt();
		CollectionColumn cc = null;
		CollectionMetadata colMetadata = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		List<CollectionColumn> list = new ArrayList<CollectionColumn>(count);
		for (int i = 0; i < count; i++) {
			int colId = buffer.readInt();
			cc = colMetadata.getCollectionColumn(colId);
			list.add(cc);
		}
		
		Index index = null;
		try {
			index = new Index(indexName, collectionName, list, unique, asc);
		} catch (InvalidIndexException e) {
		}
		IndexMetadata indexMeta = new IndexMetadata(index, null, schemaId, null, null);
		return indexMeta;
	}
}
