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

public interface FileAccess {
	/**
	 * Reads bytes from the file and returns byte array. It hides the way file
	 * is opened, in r,w,a or other way and also hides if its a RandomAccess memory 
	 * mapped file.
	 * 
	 * @param posn
	 * @param count
	 * @return
	 */
	void read(long posn, ByteBuffer buffer) throws IOException;
	
	/**
	 * Write bytes into the file.
	 * @param posn
	 * @param array
	 */
	void write(long posn, ByteBuffer buffer) throws IOException;
	
}
