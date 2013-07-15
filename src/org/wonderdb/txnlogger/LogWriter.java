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
package org.wonderdb.txnlogger;

import java.util.List;

import org.wonderdb.seralizers.block.SerializedBlock;


public class LogWriter {
	
	
	private static LogWriter instance = new LogWriter();
	
	private LogWriter() {
	}
	
	public static LogWriter getInstance() {
		return instance;
	}
	
	public void writeLog(List<SerializedBlock> blockList) {
		// check if file is full (100MB)
		// if full check how far writer has written. Until that point delete all files. and create new one from the highest position.
		
		
	}
}
