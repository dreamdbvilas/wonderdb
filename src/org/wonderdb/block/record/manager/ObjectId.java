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
package org.wonderdb.block.record.manager;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

public class ObjectId {
	Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	private static AtomicLong seed = new AtomicLong(0);
	String id = "";
	int intPadSize = String.valueOf(Integer.MAX_VALUE).getBytes().length;
	int longPadSize = String.valueOf(Long.MAX_VALUE).getBytes().length;
	public ObjectId(String machineId, boolean createNew) {
		if (createNew) {
			id = id + machineId + "_" + 
				Long.toHexString(cal.getTimeInMillis()) + "_" + 
				Long.toHexString(seed.getAndIncrement());
			byte[] bytes = id.getBytes();
			
			for (int i = 0; i < bytes.length/2; i++) {
				byte current = bytes[i];
				int posn = Math.max(0, bytes.length-i-1);
				bytes[i] = bytes[posn];
				bytes[posn] = current;
			}
			
			id = new String(bytes);
		} else {
			id = machineId;
		}
	}
	
	public int hashCode() {
		return id.hashCode();
	}
	
	public boolean equals(Object o) {
		if (this==o) {
			return true;
		}
		if (o instanceof ObjectId) {
			return ((ObjectId) o).id.equals(id);
		}
		return false;
	}
	
	public String toString() {
		return id;
	}
}
