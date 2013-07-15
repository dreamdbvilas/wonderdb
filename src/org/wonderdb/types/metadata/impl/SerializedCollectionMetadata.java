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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtrList;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.record.impl.base.BaseRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.seralizers.StringSerializer;
import org.wonderdb.seralizers.block.SerializedContinuationBlock;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.Cacheable;
import org.wonderdb.types.impl.ByteType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.StringType;


public class SerializedCollectionMetadata extends BaseRecord {
	public void setFixedColumns(String collectionName, int schemaId, boolean memoryOnly) {
		ChannelBuffer buffer = ChannelBuffers.buffer(4000);
//		buffer.writeByte(fileId);
		buffer.writeInt(schemaId);
		buffer.writeByte(memoryOnly == true ? 1 : 0);
		StringType st = new StringType(collectionName);
		StringSerializer.getInstance().toBytes(st, buffer);
//		st = new StringType(fileName);
		StringSerializer.getInstance().toBytes(st, buffer);
		byte[] bytes = new byte[buffer.readableBytes()];
		buffer.readBytes(bytes);
		ByteType bt = new ByteType(bytes);
		setColumnValue(new ColumnType(0), bt);
	}
	
	public void addColumn(CollectionColumn cc) {
		ColumnType ct = cc.getColumnType();
		if (ct.getValue() instanceof Integer) {
			int val = (Integer) ct.getValue();
			ct = new ColumnType(val+3);
			cc = new CollectionColumn(cc.getColumnName(), val, cc.getCollectionColumnSerializerName(), cc.isNullable(), cc.isQueriable());
		} else {
//			throw new RuntimeException("Invalid column: " + cc);
		}
		setColumnValue(ct, cc);
	}
	
	public void addColumns(List<CollectionColumn> list) {
		if (list == null || list.size() == 0) {
			return;
		}
		for (int i = 0; i < list.size(); i++) {
			addColumn(list.get(i));
		}
	}
	
	public void updateHead(Shard shard, BlockPtr head, TransactionId txnId) {
		if (head == null) {
			head = new SingleBlockPtr((byte) -1 , -1);
		}
		if (shard.getSchemaId() < 3) {
//			FixedColumns fc = getFixedColumns();
			setColumnValue(new ColumnType(1), head);
		} else {
			updateShardHead(shard, head, txnId);
		}
	}
	
	private void updateShardHead(Shard shard, BlockPtr head, TransactionId txnId) {
		byte fileId = (byte) FileBlockManager.getInstance().getId(shard);
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>(); 
		try {
			IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, -1, pinnedBlocks);
			if (ibb.getData().size() > 0) {
				ibb.getData().set(0, head);
			} else {
				ibb.getData().add(head);
			}		
			SerializedContinuationBlock scb = CacheObjectMgr.getInstance().updateIndexBlock(ibb, -1, null);
			SecondaryCacheHandlerFactory.getInstance().getCacheHandler().changed(ibb.getPtr());
			LogManager.getInstance().logBlock(txnId, scb.getBlocks().get(0));
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public void updateTail(Shard shard, SingleBlockPtrList list, TransactionId txnId) {
//		FixedColumns fc = getFixedColumns();
		if (shard.getSchemaId() < 3) {
			setColumnValue(new ColumnType(2), list);
		} else {
			updateShardTail(shard, list, txnId);
		}		
	}
	
	private void updateShardTail(Shard shard, SingleBlockPtrList list, TransactionId txnId) {		
		byte fileId = (byte) FileBlockManager.getInstance().getId(shard);
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();

		try {
			IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, -1, pinnedBlocks);
			List<Cacheable> l = new ArrayList<Cacheable>(ibb.getData());
			BlockPtr head = new SingleBlockPtr((byte) -1, -1);
			if (l.size() > 0) {
				head = (BlockPtr) l.get(0);
			}
			l.clear();
			l.add(head);
			l.addAll(list);
			ibb.getData().clear();
			ibb.getData().addAll(l);
			SerializedContinuationBlock scb = CacheObjectMgr.getInstance().updateIndexBlock(ibb, -1, pinnedBlocks);
			SecondaryCacheHandlerFactory.getInstance().getCacheHandler().changed(ibb.getPtr());
			LogManager.getInstance().logBlock(txnId, scb.getBlocks().get(0));
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public BlockPtr getHead(Shard shard) {
		FixedColumns fc = getFixedColumns();
		int schemaId = fc == null ? shard.getSchemaId() : fc.schemaId;
		BlockPtr p = null;
		if (schemaId < 3) {
			p = (BlockPtr) getColumnValue(null, new ColumnType(1), null);
		} else {
			Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
			try {
				byte fileId = (byte) FileBlockManager.getInstance().getId(shard);
				p = new SingleBlockPtr(fileId, 0);
				if (p != null && p.getFileId() >= 0 && p.getBlockPosn() >= 0) {
					IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, -1, pinnedBlocks);
					p = (BlockPtr) ibb.getData().getUncached(0);
				}
			} finally {
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
		if (p == null || p.getFileId() < 0 || p.getBlockPosn() < 0) {
			return null;
		}
		return p;
	}
	
	public SingleBlockPtrList getTail(Shard shard) {
		FixedColumns fc = getFixedColumns();
		BlockPtr p = null;
		SingleBlockPtrList sbpl = new SingleBlockPtrList();
		int schemaId = fc == null ? shard.getSchemaId() : fc.schemaId;
		if (schemaId < 3) {
			sbpl = (SingleBlockPtrList) getColumnValue(null, new ColumnType(2), null);
		} else {
			Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
			try {
				byte fileId = (byte) FileBlockManager.getInstance().getId(shard);
				p = new SingleBlockPtr(fileId, 0);
				if (p != null && p.getFileId() >= 0 && p.getBlockPosn() >= 0) {
					IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, -1, pinnedBlocks);
					for (int i = 1; i < ibb.getData().size(); i++) {
						sbpl.add((BlockPtr) ibb.getData().getUncached(i));
					}
				}
			} finally {
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
		}
		return sbpl;
	}
	
	public FixedColumns getFixedColumns() {
		FixedColumns fc = new FixedColumns();
		ByteType bt = (ByteType) getColumnValue(null, new ColumnType(0), null);
		if (bt == null) {
			return null;
		}
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(bt.get());
//		fc.fileId = buffer.readByte();
		fc.schemaId = buffer.readInt();
		fc.memoryOnly = buffer.readByte() == 1 ? true : false;
		StringType st = StringSerializer.getInstance().unmarshal(buffer);
		fc.collectionName = st.get();
		st = StringSerializer.getInstance().unmarshal(buffer);
//		fc.fileName = st.get();
		return fc;
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
			if (id >= 3) {
				list.add((CollectionColumn) getColumnValue(null, ct, null));
			}
		}
		return list;
	}
	
	public static class FixedColumns {
//		public byte fileId;
		public int schemaId;
		public boolean memoryOnly;
//		public String fileName;
		public String collectionName;
	}

	@Override
	public String getSerializerName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getByteSize() {
		return 0;
	}

	public static ColumnType adjustId(ColumnType ct) {
		if (ct.getValue() instanceof Integer) {
			int val = (Integer) ct.getValue();
			return new ColumnType(val + 3);
		}
		throw new RuntimeException("Invalid ColumnType" + ct.getValue());
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
