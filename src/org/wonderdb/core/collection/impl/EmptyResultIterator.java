package org.wonderdb.core.collection.impl;

import java.util.Set;

import org.wonderdb.block.Block;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.Record;

public class EmptyResultIterator extends BTreeIteratorImpl {
	private static EmptyResultIterator instance = new EmptyResultIterator();
	
	private EmptyResultIterator() {
		super(null, null, false, false, null);
	}
	
	public static EmptyResultIterator getInstance() {
		return instance;
	}
	
	@Override
	public void lock(Block block) {
	}

	@Override
	public void unlock() {
	}

	@Override
	public void unlock(boolean shouldUnpin) {
	}

	@Override
	public void insert(Record data) {
	}

	@Override
	public boolean isAnyBlockEmpty() {
		return false;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Record next() {
		return null;
	}

	@Override
	public void remove() {
	}

	@Override
	public Block getCurrentBlock() {
		return null;
	}

	@Override
	public Set<Object> getPinnedSet() {
		return null;
	}

	@Override
	public TypeMetadata getTypeMetadata() {
		return null;
	}

	@Override
	public Record peek() {
		// TODO Auto-generated method stub
		return null;
	}
}
