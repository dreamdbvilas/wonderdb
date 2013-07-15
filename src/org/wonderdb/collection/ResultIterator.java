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
package org.wonderdb.collection;

import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.types.Cacheable;


public interface ResultIterator {
	public void lock(Block block);
	public void unlock();
	public void unlock(boolean shouldUnpin);
	public int insert(Cacheable data);
	public boolean isAnyBlockEmpty();
	public boolean hasNext();
	public ResultContent next();
	public void remove();
	public Block getCurrentBlock();
	public Cacheable nextEntry();
	public Set<BlockPtr> getPinnedSet();
}
