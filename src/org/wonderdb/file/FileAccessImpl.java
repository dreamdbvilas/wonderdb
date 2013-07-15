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

import java.io.IOException;
import java.nio.ByteBuffer;



public class FileAccessImpl implements FileAccess {
	String fileName;
	boolean useChannel;
	
	public FileAccessImpl(String fileName, boolean useChannel) {
		this.fileName = fileName;
		this.useChannel = useChannel;
	}
	
	public void read(long posn, ByteBuffer buffer) throws IOException {
		if (useChannel) {
			FilePointerFactory.getInstance().readChannel(fileName, posn, buffer);
		} else {
//			FilePointerFactory.getInstance().readFile(fileName, posn, buffer);
		}
	}
	
	public void write(long posn, ByteBuffer buffer) throws IOException {
		if (useChannel) {
			FilePointerFactory.getInstance().writeChannel(fileName, posn, buffer);
		} else {
			FilePointerFactory.getInstance().writeFile(fileName, posn, buffer);
		}		
	}		
}
