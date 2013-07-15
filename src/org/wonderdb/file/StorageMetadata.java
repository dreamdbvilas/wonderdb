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
package org.wonderdb.file;

import java.util.concurrent.ConcurrentHashMap;

public class StorageMetadata {
	private static int nextId = 0;
	private String defaultFileName = null;
	private String systemsFileName = null;
	
	ConcurrentHashMap<String, Integer> fileToIdMap = new ConcurrentHashMap<String, Integer>();
	ConcurrentHashMap<Integer, String> idToFileMap = new ConcurrentHashMap<Integer, String>();
	private static StorageMetadata instance = new StorageMetadata();
	
	private StorageMetadata() {
	}
	
	public static StorageMetadata getInstance() {
		return instance;
	}
	
	public String getFileName(int id) {
		String fileName = idToFileMap.get(id);
		if (fileName == null) {
			if (defaultFileName == null) {
				throw new RuntimeException("Default storage file name is not set");
			}
			fileName = defaultFileName;
		}
		return fileName;
	}
	
	public int getId(String fileName) {
		Integer id = fileToIdMap.get(fileName);
		if (id == null) {
			throw new RuntimeException("Invalid file name: " + fileName);
		}
		return id;
	}
	
	public void setDefault(String fileName) {
		Integer id = fileToIdMap.get(fileName);
		if (id == null) {
			add(fileName);
		}
		defaultFileName = fileName;
	}
	
	public void setSystemsFileName(String fileName) {
		Integer id = fileToIdMap.get(fileName);
		if (id == null) {
			add(fileName);
		}
		systemsFileName = fileName;		
	}
	
	public String getDefaultFileName() {
		if (defaultFileName == null) {
			throw new RuntimeException("Default storage file is not set");
		}
		return defaultFileName;
	}
	
	public String getSystemsFileName() {
		if (systemsFileName == null) {
			throw new RuntimeException("Systems storage file is not set");
		}
		return systemsFileName;
	}
	
	public void add(String fileName) {
		if (fileToIdMap.containsKey(fileName)) {
			throw new RuntimeException("File name already exists: " + fileName);
		}
		
		int id = nextId++;
		if (defaultFileName == null) {
			defaultFileName = fileName;
		}
		
		if (systemsFileName == null) {
			systemsFileName = fileName;
		}
		
		fileToIdMap.putIfAbsent(fileName, id);
		idToFileMap.putIfAbsent(id, fileName);
	}
}
