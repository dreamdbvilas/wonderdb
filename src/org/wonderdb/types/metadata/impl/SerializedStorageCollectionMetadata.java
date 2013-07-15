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
package org.wonderdb.types.metadata.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.record.impl.base.BaseRecord;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.FileBlockEntryType;
import org.wonderdb.seralizers.StringSerializer;
import org.wonderdb.types.impl.ByteType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.LongType;
import org.wonderdb.types.impl.StringType;


public class SerializedStorageCollectionMetadata extends BaseRecord {
//	Map<ColumnType, DBType> allColumns = new HashMap<ColumnType, DBType>();
//	int posn = -1;
//	long lastAccessed = -1;
//	long lastUpdated = -1;

//	@Override
//	public Map<ColumnType, DBType> getQueriableColumns() {
//		return allColumns;
//	}
//
//	@Override
//	public int getPosn() {
//		return posn;
//	}
//
//	@Override
//	public void setPosn(int p) {
//		this.posn = p;
//	}
//
//	@Override
//	public Map<ColumnType, DBType> getColumns() {
//		return allColumns;
//	}
//
//	@Override
//	public void setColumns(Map<ColumnType, DBType> colValues) {
//		allColumns.putAll(colValues);
//	}
//
//	@Override
//	public DBType getColumnValue(ColumnType name) {
//		return allColumns.get(name);
//	}
//
//	@Override
//	public void setColumnValue(ColumnType name, DBType value) {
//		allColumns.put(name, value);
//	}
//
//	@Override
//	public void removeColumn(ColumnType name) {
//		allColumns.remove(name);
//	}

	public void setFileBlockEntryType(FileBlockEntryType entry) {
		ChannelBuffer buffer = ChannelBuffers.buffer(4000);
		buffer.writeByte(entry.getFileId());
		buffer.writeInt(entry.getBlockSize());
		buffer.writeByte(entry.isDefault() == true ? (byte) 1 : (byte) 0);
		StringType st = new StringType(entry.getFileName());
		StringSerializer.getInstance().toBytes(st, buffer);
		byte[] bytes = new byte[buffer.readableBytes()];
		buffer.readBytes(bytes);
		ByteType bt = new ByteType(bytes);
		setColumnValue(new ColumnType(0), bt);
	}
	
	public FileBlockEntryType getFileBlockEntryType() {
		ByteType bt = (ByteType) getColumnValue(null, new ColumnType(0), null);
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(bt.get());
		byte fileId = buffer.readByte();
		int blockSize = buffer.readInt();
		boolean isDefault = buffer.readByte() == 1 ? true : false;
		String fileName = StringSerializer.getInstance().unmarshal(buffer).get();
		FileBlockEntryType entry = new FileBlockEntryType(fileName, blockSize, getCurrentFilePosn());
		entry.setFileId(fileId);
		entry.setDefault(isDefault);
		return entry;
	}
	
	public void addColumns(List<CollectionColumn> ccList) {
		for (CollectionColumn cc: ccList) {
			setColumnValue(cc.getColumnType(), cc);
		}
	}
	
	public void updateCurrentFilePosn(long posn) {
		setColumnValue(new ColumnType(1), new LongType(posn));
	}
	
	public long getCurrentFilePosn() {
		LongType lt = (LongType) getColumnValue(null, new ColumnType(1), null);
		return lt.get();
	}
	
	public List<CollectionColumn> getCollectionColumns() {
		List<CollectionColumn> list = new ArrayList<CollectionColumn>();
		Iterator<ColumnType> iter = super.getColumns().keySet().iterator();
		while (iter.hasNext()) {
			ColumnType ct = iter.next();
			int id = -1;
			if (ct.getValue() instanceof Integer) {
				id = (Integer) ct.getValue();
			}
			if (id >= 0) {
				list.add((CollectionColumn) getColumnValue(null, ct, null));
			}
		}
		return list;
	}

	@Override
	public String getSerializerName() {
		return null;
	}

	@Override
	public int getByteSize() {
		return 0;
	}

//
//	@Override
//	public long getLastAccessDate() {
//		return lastAccessed;
//	}
//
//	@Override
//	public long getLastModifiedDate() {
//		return lastUpdated;
//	}	
//	
//
//	@Override
//	public void updateLastAccessDate() {
//		lastAccessed = System.currentTimeMillis();
//	}
//
//	@Override
//	public void updateLastModifiedDate() {
//		lastUpdated = System.currentTimeMillis();
//		lastAccessed = lastUpdated;
//	}	
}
