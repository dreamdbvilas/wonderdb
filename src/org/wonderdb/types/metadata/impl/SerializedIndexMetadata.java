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
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.record.impl.base.BaseRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.seralizers.MetaIndexSerializer;
import org.wonderdb.seralizers.block.SerializedContinuationBlock;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.impl.ByteType;
import org.wonderdb.types.impl.ColumnType;


public class SerializedIndexMetadata extends BaseRecord {
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

	public void setIndexMetaFixedColumns(IndexMetadata indexMeta) {
		ChannelBuffer buffer = ChannelBuffers.buffer(4000);
		MetaIndexSerializer.getInstance().toBytes(indexMeta, buffer);
		byte[] bytes = new byte[buffer.readableBytes()];
		buffer.readBytes(bytes);
		ByteType bt = new ByteType(bytes);
		setColumnValue(new ColumnType(0), bt);
	}
	
	public void addColumns(List<CollectionColumn> ccList) {
		for (CollectionColumn cc: ccList) {
			setColumnValue(cc.getColumnType(), cc);
		}
	}
	
	public void updateHeadRoot(byte fileId, BlockPtr head, BlockPtr root, TransactionId txnId, Set<BlockPtr> pinnedBlocks) {
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, -1, pinnedBlocks);
		if (ibb.getData().size() > 0) {
			ibb.getData().set(0, head);
		} else {
			ibb.getData().add(head);
		}

		if (ibb.getData().size() > 1) {
			ibb.getData().set(1, root);
		} else {
			ibb.getData().add(root);
		}
		
		SerializedContinuationBlock scb = CacheObjectMgr.getInstance().updateIndexBlock(ibb, -1, pinnedBlocks);
		SecondaryCacheHandlerFactory.getInstance().getCacheHandler().changed(ibb.getPtr());
		LogManager.getInstance().logBlock(txnId, scb.getBlocks().get(0));
	}
	
	public BlockPtr getHead(Shard shard) {
//		IndexMetadata idxMeta = getIndexMetaFixedColumns();
		byte fileId = (byte) FileBlockManager.getInstance().getId(shard);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		try {
			IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, shard.getSchemaId(), pinnedBlocks);
			if (ibb.getData().size() > 0) {
				p = (BlockPtr) ibb.getData().getUncached(0);
			}
			if (p == null || p.getFileId() < 0 || p.getBlockPosn() < 0) {
				return null;
			}
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return p;
	}
	
	public BlockPtr getRoot(Shard shard) {
//		IndexMetadata idxMeta = getIndexMetaFixedColumns();
		byte fileId = (byte) FileBlockManager.getInstance().getId(shard);
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		try	{
			IndexBranchBlock ibb = (IndexBranchBlock) CacheObjectMgr.getInstance().getIndexBlock(p, shard.getSchemaId(), pinnedBlocks);
			if (ibb.getData().size() > 1) {
				p = (BlockPtr) ibb.getData().getUncached(1);
			}
			
			if (p == null || p.getFileId() < 0 || p.getBlockPosn() < 0) {
				return null;
			}
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return p;
	}
	
	public IndexMetadata getIndexMetaFixedColumns() {
		ByteType bt = (ByteType) getColumnValue(null, new ColumnType(0), null);
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(bt.get());
		return MetaIndexSerializer.getInstance().unmarshal(buffer);
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
