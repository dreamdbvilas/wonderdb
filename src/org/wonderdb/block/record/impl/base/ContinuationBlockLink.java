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

import org.wonderdb.block.record.manager.RecordId;

public class ContinuationBlockLink {
	RecordId linkRecordId;
	int byteSize = 0;
	
	public ContinuationBlockLink(RecordId id, int size) {
		this.linkRecordId = id;
		this.byteSize = size;
	}
	
	public RecordId LinkRecordId() {
		return linkRecordId;
	}
	
	public void removeLinkRecordId() {
		linkRecordId = null;
	}
	
	public int getByteSize() {
		return byteSize;
	}
	
	public void setByteSize(int size) {
		this.byteSize = size;
	}
}
