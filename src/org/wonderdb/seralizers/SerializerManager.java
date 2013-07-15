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

import java.util.HashMap;
import java.util.Map;

import org.wonderdb.types.SerializableType;


public class SerializerManager {
	private Map<String, CollectionColumnSerializer<? extends SerializableType>> map = new HashMap<String, CollectionColumnSerializer<? extends SerializableType>>();
	private static SerializerManager instance = new SerializerManager();
	
	private SerializerManager() {
		register("is", IntSerializer.getInstance());
		register("ls", LongSerializer.getInstance());
		register("ds", DoubleSerializer.getInstance());
		register("fs", FloatSerializer.getInstance());
		register("ss", StringSerializer.getInstance());
		register("cc", MetaCollectionColumnSerializer.getInstance());
		register("bs", BlockPtrSerializer.getInstance());
		register("bl", BlockPtrListSerializer.getInstance());
		register("bt", ByteSerializer.getInstance());
	}
	
	public static SerializerManager getInstance() {
		return instance;
	}
	
	public void register(String serializerName, CollectionColumnSerializer<? extends SerializableType> serializer) {
		map.put(serializerName, serializer);
	}
	
	public CollectionColumnSerializer<? extends SerializableType> getSerializer(String name) {
		return map.get(name);
	}
}
