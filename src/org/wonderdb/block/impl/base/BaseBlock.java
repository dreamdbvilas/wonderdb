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
package org.wonderdb.block.impl.base;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.wonderdb.block.Block;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.schema.StorageUtils;
import org.wonderdb.seralizers.BlockPtrSerializer;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.SerializableType;


public abstract class BaseBlock implements Block {
	public static final int BASE_SIZE = Integer.SIZE/8 /* count */ + (2 * BlockPtrSerializer.BASE_SIZE);
	private CacheableList entries = null;
	int schemaObjectId = -1;
	ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
//	protected long size = -1;
	BlockPtr ptr = null;
	protected int bufferCount = 1;
	BlockPtr nextPtr = null;
	BlockPtr prevPtr = null;
	
//	private static CacheHandler<CacheableList> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
//	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
//	private static PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();
//	private static CacheResourceProvider<SerializedBlock> secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	
	public BaseBlock(BlockPtr ptr, int schemaObjectId) {
		this.schemaObjectId = schemaObjectId;
		this.ptr = ptr;
		entries = new BaseCacheableList(this, schemaObjectId);
	}
	
	public BlockPtr getPtr() {
		return ptr;
	}
	
	public BlockPtr getNext() {
		return nextPtr;
	}
	
	public BlockPtr getPrev() {
		return prevPtr;
	}
	
	public void setNext(BlockPtr n) {
		this.nextPtr = n;
	}
	
	public void setPrev(BlockPtr n) {
		this.prevPtr = n;
	}
	
	public BlockPtr getParent() {
		throw new RuntimeException("Method not supported");
	}
	
	public void setParent(BlockPtr n) {
		throw new RuntimeException("Method not supported");
	}
	
	public CacheableList getData() {
		return entries;
	}
	
	public void writeLock() {
		rwLock.writeLock().lock();
	}
	
	public void writeUnlock() {
		rwLock.writeLock().unlock();
	}
	
	public void readLock() {
		rwLock.readLock().lock();
	}
	
	public void readUnlock() {
		rwLock.readLock().unlock();
	}
	
	public int getSchemaObjectId() {
		return schemaObjectId;
	}
	
	public void setSchemaObjectId(int id) {
		schemaObjectId = id;
	}
	
//	public void addToSize(long s) {
//		size = size + s;
//	}
	
//	public void removeFromSize(long s) {
//		size = size - s;
//		primaryResourceProvider.returnResource((int) s);
//	}
	
//	public void resetSize() {
//		size = -1;
//		size = getByteSize();
//	}	
	
	public int addEntry(int posn, Cacheable c) {
		CacheableList list = getData();
		int addPosn = posn;
		
		if (list.size() <= posn || posn < 0) {
			list.add(c);
			addPosn = list.size()-1;
		} else {
			list.add(posn, c);
		}
		return addPosn;
	}
	
	public void clear() {
		entries.clear();
	}
	
//	public boolean isNew() {
//		return isNewFlag;
//	}
//	
//	public void newBlock() {
//		isNewFlag = true;
//	}
//	
	@Override
	public int getByteSize() {
		int size = BASE_SIZE;
		for (int i = 0; i < entries.size(); i++) {
			SerializableType st = (SerializableType) entries.getUncached(i);
			size = size + st.getByteSize();
		}
		return size;
	}
	
	@Override
	public long getLastAccessTime() {
		return -1;
	}
	
	@Override
	public void setLastAccessTime(long t) {
	}
//	
//	@Override
//	public long getLastModifiedTime() {
//		return -1;
//	}
//	
//	@Override
//	public void setLastModifiedTime(long t) {
//	}
//	
	@Override
	public int compareTo(BlockPtr ptr) {
		return this.ptr.compareTo(ptr);
	}
	
	@Override
	public int getBlockCount() {
		return StorageUtils.getInstance().getSmallBlockCount(getByteSize());
	}
	
	@Override 
	public int getBufferCount() {
		return bufferCount;
	}
	
	@Override
	public Block buildReference() {
		return this;
	}
//	@Override
//	public void setBufferCount(int count) {
//		bufferCount = count;
//	}
}
