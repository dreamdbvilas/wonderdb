package org.wonderdb.block;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.record.Record;

public abstract class BaseBlock implements Block {

	private List<Record> records = new ArrayList<>();
	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

	BlockPtr ptr = null;
	BlockPtr nextPtr = null;
	BlockPtr prevPtr = null;
	
	public BaseBlock(BlockPtr ptr) {
		this.ptr = (BlockPtr) ptr.clone();//.copyOf();
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
		this.nextPtr = n != null ? (BlockPtr) n.clone() : null;
	}
	
	public void setPrev(BlockPtr n) {
		this.prevPtr = n != null ? (BlockPtr) n.clone() : null;
	}
	
	public BlockPtr getParent() {
		throw new RuntimeException("Method not supported");
	}
	
	public void setParent(BlockPtr n) {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public List<Record> getData() {
		return records;
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
	
	@Override
	public long getLastAccessTime() {
		return -1;
	}
	
	@Override
	public void setLastAccessTime(long t) {
	}

	@Override
	public int compareTo(BlockPtr ptr) {
		return this.ptr.compareTo(ptr);
	}
	
	@Override
	public void setData(List<Record> records) {
		this.records = records;
	}
	
	@Override
	public int hashCode() {
		return this.getPtr().hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof BlockPtr) {
			return this.getPtr().equals(o);
		}
		
		if (o instanceof Block) {
			return this.getPtr().equals(((Block) o).getPtr());
		}
		
		return false;
	}
	
	@Override
	public List<Record> getFull() {
		throw new RuntimeException("Method not supported");
	}
}
