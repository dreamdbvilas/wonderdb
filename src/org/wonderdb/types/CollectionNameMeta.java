package org.wonderdb.types;


public class CollectionNameMeta implements DBType {
	String name;
	BlockPtr head;
	RecordId recordId;
	int concurrency = 0;
	boolean isLoggingEnabled = true;

	public int getConcurrency() {
		return concurrency;
	}
	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}
	public boolean isLoggingEnabled() {
		return isLoggingEnabled;
	}
	public void setLoggingEnabled(boolean isLoggingEnabled) {
		this.isLoggingEnabled = isLoggingEnabled;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public BlockPtr getHead() {
		return head;
	}
	public void setHead(BlockPtr head) {
		this.head = head;
	}
	public RecordId getRecordId() {
		return recordId;
	}
	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}
	
	@Override
	public int compareTo(DBType o) {
		throw new RuntimeException("Mehtod not supported");
	}
	
	@Override
	public DBType copyOf() {
		throw new RuntimeException("Mehtod not supported");
	}
}
