package org.wonderdb.types;

import java.util.List;

import org.wonderdb.cluster.Shard;
import org.wonderdb.core.collection.BTree;

public class IndexNameMeta implements DBType {
	int id;
	String indexName;
	String collectionName;
	List<Integer> columnIdList;
	BlockPtr head;
	RecordId recordId;
	boolean unique = false;
	boolean isAscending = true;
	BTree tree = null;
	byte indexType;
	
	public BTree getTree() {
		return tree;
	}

	public void setTree(BTree tree) {
		this.tree = tree;
	}

	public boolean isAscending() {
		return isAscending;
	}

	public void setAscending(boolean isAscending) {
		this.isAscending = isAscending;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	
	public List<Integer> getColumnIdList() {
		return columnIdList;
	}
	
	public void setColumnIdList(List<Integer> columnIdList) {
		this.columnIdList = columnIdList;
	}
	
	public RecordId getRecordId() {
		return recordId;
	}
	
	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}

	public BlockPtr getHead() {
		return head;
	}

	public void setHead(BlockPtr head) {
		this.head = head;
	}

	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Mehtod not supported");
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Mehtod not supported");
	}
	
	@Override
	public int hashCode() {
		throw new RuntimeException("Mehtod not supported");
	}
	
	@Override
	public boolean equals(Object o) {
		throw new RuntimeException("Mehtod not supported");
	}
	public BTree getIndexTree(Shard shard) {
		return tree;
	}	

	public byte getIndexType() {
		return indexType;
	}

	public void setIndexType(byte indexType) {
		this.indexType = indexType;
	}
}
