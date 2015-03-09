package org.wonderdb.types.record;

import org.wonderdb.types.RecordId;

public abstract interface ListRecord extends Record {
	public RecordId getRecordId();
	public void setRecordId(RecordId recordId);
}
