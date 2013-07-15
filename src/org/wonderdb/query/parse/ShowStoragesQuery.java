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
package org.wonderdb.query.parse;

import java.util.List;

import org.wonderdb.expression.AndExpression;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.FileBlockEntryType;


public class ShowStoragesQuery extends BaseDBQuery {
	
	public ShowStoragesQuery(){
		super(null, -1, null);
	}
	
	
	public String execute() {
		List<FileBlockEntryType> entries = FileBlockManager.getInstance().getFileBlockEntries();
		if (entries == null) {
			return "\n";
		}
		StringBuilder builder = new StringBuilder();
		for (FileBlockEntryType entry : entries) {
			builder.append("-------------------------------------------------------------------------\n");
			builder.append("File Name: ").append(entry.getFileName()).append("\n");
			builder.append("Block Size: ").append(entry.getBlockSize()).append("\n");
			builder.append("Is Default: ").append(entry.isDefault()).append("\n");
			builder.append("\n");
		}
		return builder.toString();
	}


	@Override
	public AndExpression getExpression() {
		return null;
	}
}