package org.wonderdb.serialize;
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

import java.util.HashMap;
import java.util.Map;

import org.wonderdb.serialize.metadata.CollectionNameMetaSerializer;
import org.wonderdb.serialize.metadata.ColumnNameMetaSerializer;
import org.wonderdb.serialize.metadata.FileBlockEntrySerializer;
import org.wonderdb.serialize.metadata.IndexNameMetaSerializer;



public class SerializerManager {
	public static final int STRING = 1;
	public static final int INT = 2;
	public static final int LONG = 3;
	public static final int DOUBLE = 4;
	public static final int FLOAT = 5;
	public static final int BLOCK_PTR = 6;
	public static final int INDEX_TYPE = 7;
	public static final int BLOCK_PTR_LIST_TYPE = 8;
	public static final int FILE_BLOCK_ENRTY_TYPE = 9;
	public static final int COLLECTION_NAME_META_TYPE = 10;
	public static final int COLUMN_NAME_META_TYPE = 11;
	public static final int INDEX_NAME_META_TYPE = 12;
	public static final int BYTE_ARRAY_TYPE = 13;
	
	private Map<Integer, TypeSerializer> map = new HashMap<>();
	private static SerializerManager instance = new SerializerManager();
	
	private SerializerManager() {
		register(STRING, DefaultSerializer.getInstance());
		register(INT, DefaultSerializer.getInstance());
		register(LONG, DefaultSerializer.getInstance());
		register(DOUBLE, DefaultSerializer.getInstance());
		register(FLOAT, DefaultSerializer.getInstance());
		register(BLOCK_PTR, DefaultSerializer.getInstance());
		register(BLOCK_PTR_LIST_TYPE, DefaultSerializer.getInstance());
		register(INDEX_TYPE, IndexSerializer.getInstance());
		register(FILE_BLOCK_ENRTY_TYPE, FileBlockEntrySerializer.getInstance());
		register(COLLECTION_NAME_META_TYPE, CollectionNameMetaSerializer.getInstance());
		register(COLUMN_NAME_META_TYPE, ColumnNameMetaSerializer.getInstance());
		register(INDEX_NAME_META_TYPE, IndexNameMetaSerializer.getInstance());
		register(BYTE_ARRAY_TYPE, DefaultSerializer.getInstance());
	}
	
	public static SerializerManager getInstance() {
		return instance;
	}
	
	public void register(int type, TypeSerializer serializer) {
		map.put(type, serializer);
	}
	
	public TypeSerializer getSerializer(int type) {
		return map.get(type);
	}
}
