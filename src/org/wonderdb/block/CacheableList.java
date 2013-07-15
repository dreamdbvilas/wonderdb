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
package org.wonderdb.block;


import java.util.List;
import java.util.Set;

import org.wonderdb.types.Cacheable;


public interface CacheableList extends Cacheable, List<Cacheable> {
	Cacheable  getUncached(int posn);
//	public void add(Cacheable c);
//	public void add(int i, Cacheable c);
//	public void set(int i, Cacheable c);
//	int size();
//	public Cacheable getAndIgnore(int i);
	public Cacheable getAndPin(int i, Set<BlockPtr> pinnedBlocks);
//	public Cacheable remove(int i);
//	boolean addAll(List<? extends Cacheable> list);
//	boolean addAll(int posn, List<? extends Cacheable> list);
//	public void clear();
}
