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
package org.wonderdb.block.record.impl.base;

import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.StringType;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;


public class JSONValueExtractor implements RecordValueExtractor {
	
	private static JSONValueExtractor instance = new JSONValueExtractor();
	
	private JSONValueExtractor() {
	}
	
	public static JSONValueExtractor getInstance() {
		return instance;
	}

	@Override
	public DBType extract(String path, DBType dtvalue, String rootNode) {
		String value = null;
		if (dtvalue instanceof StringType) {
			value = ((StringType) dtvalue).get();
		}
		
		if (value == null) {
			return null;
		}
		
		JsonParser parser = new JsonParser();
		JsonElement je = null;
		try {
			je = parser.parse(value);			
		} catch (JsonSyntaxException e1) {
			return null;
		} catch (JsonParseException e2) {
			return null;
		}
		String[] keys = path.split("/");
		String val = null;
		JsonObject jo = null;

		for (int i = 1; i < keys.length; i++) {
			String key = keys[i];			
			if (je == null || je.isJsonNull()) {
				break;
			}
			if (je.isJsonPrimitive()) {
				je = null;
				break;
			}
			jo = je.getAsJsonObject();
			je = jo.get(key);
		}
		
		JsonObject outJo = new JsonObject();
		
		if (rootNode != null && rootNode.length() > 0) {
			outJo.add(rootNode, je);
			val = outJo.getAsString();
		} else {
			if (je != null) {
				if (je.isJsonObject()) {
					jo = je.getAsJsonObject();
					val = jo.toString();
				} else {
					val = je.getAsString();
				}
			}
		}
		
		return new StringType(val);
	}
}
