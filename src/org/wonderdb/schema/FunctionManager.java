package org.wonderdb.schema;


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

import org.wonderdb.parser.jtree.SimpleNode;



public class FunctionManager {
	private Map<String, WonderDBFunction> map = new HashMap<String, WonderDBFunction>();
	private static FunctionManager instance = new FunctionManager();
	
	private FunctionManager() {
//		register("count", new CountFunction(null, null));
	}
	
	public static FunctionManager getInstance() {
		return instance;
	}
	
	public void register(String functionName, WonderDBFunction function) {
		map.put(functionName, function);
	}
	
	public WonderDBFunction getFunction(String name, SimpleNode node) {
		WonderDBFunction fn = map.get(name);
		return fn.newInstance(node);
	}
}
