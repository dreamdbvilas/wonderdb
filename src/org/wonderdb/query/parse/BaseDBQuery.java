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

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.server.WonderDBShardServerHandler;


public abstract class BaseDBQuery implements DBQuery {
	int type = -1;
	String query = null;
	ChannelBuffer buffer = null;
	
	public BaseDBQuery(String query, int type, ChannelBuffer buffer) {
		this.query = query;
		this.type = type;
		this.buffer = buffer;
	}
	
	@Override
	public String getQueryString() {
		return query;
	}
	
	@Override
	public ChannelBuffer getRawBuffer() {
		return buffer;
	}
	
	@Override
	public boolean executeLocal() {
		if (WonderDBShardServerHandler.SERVER_HANDLER == type) {
			return true;
		}
		return false;
	}
}
