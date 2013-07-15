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
package org.wonderdb.schema;

import org.wonderdb.block.record.impl.base.BaseRecord;


public abstract class SchemaObjectImpl extends BaseRecord implements SchemaObject {
	String name;
//	String fileName;
	String serializerName;
	int schemaId;
//	byte fileId;
	
	SchemaObjectImpl(String name, String serializerName, int schemaId) {
		this.name = name;
//		this.fileName = fileName;
		this.serializerName = serializerName;
		this.schemaId = schemaId;
//		this.fileId = (byte) FileBlockManager.getInstance().getId(fileName);
	}
	
	public String getName() {
		return name;
	}
	
//	public String getFileName() {
//		return fileName;
//	}
	
	public String getSerialierName() {
		return serializerName;
	}
	
	public int getSchemaId() {
		return schemaId;
	}	
	
//	public byte getFileId() {
//		return fileId;
//	}
}
