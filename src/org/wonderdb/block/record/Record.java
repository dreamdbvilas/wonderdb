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
package org.wonderdb.block.record;
import org.wonderdb.block.index.impl.base.Queriable;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.SerializableType;


public interface Record extends SerializableType, Queriable, Cacheable {
//	RecordBlock getParent();
	int getPosn();
	public void updateContents(Object o);
	public void removeContents(Object o);
	void setPosn(int p);
}
