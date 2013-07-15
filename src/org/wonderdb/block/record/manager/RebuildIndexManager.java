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
package org.wonderdb.block.record.manager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.index.impl.base.IndexCompareIndexQuery;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.BTree;
import org.wonderdb.collection.ResultIterator;
import org.wonderdb.collection.exceptions.UniqueKeyViolationException;
import org.wonderdb.query.parse.DBSelectQuery;
import org.wonderdb.query.parse.QueryParser;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;

public class RebuildIndexManager {
	private static RebuildIndexManager instance = new RebuildIndexManager();
	
	private RebuildIndexManager() {
	}
	
	public static RebuildIndexManager getInstance() {
		return instance;
	}
	
	public void rebuild(int idxId) {
		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex(idxId);
		List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(idxMeta.getIndex().getCollectionName());
		for (int x = 0; x < shards.size(); x++) {
			@SuppressWarnings("unused")
			BTree tree = idxMeta.getIndexTree(shards.get(x));
			String collectionName = idxMeta.getIndex().getCollectionName();
			int colSchemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
			
			List<CollectionColumn> colList = idxMeta.getIndex().getColumnList();
			
			StringBuilder selCols = new StringBuilder();
			List<ColumnType> loadColumns = new ArrayList<ColumnType>();
			
			for (int i = 0; i < colList.size(); i++) {
				CollectionColumn cc = colList.get(i);
				String colName = cc.getColumnName();
				loadColumns.add(cc.getColumnType());
				if (i != 0) {
					selCols.append(",");
				}
				selCols.append(colName);
			}
			
			String query = "select " + selCols.toString() + " from " + collectionName;
			DBSelectQuery q = (DBSelectQuery) QueryParser.getInstance().parse(query);
			List<Map<CollectionAlias, RecordId>> results = q.executeGetDC(shards.get(x));
			Iterator<RecordId> iter = results.get(0).values().iterator();
			Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
			
			while (iter.hasNext()) {
				RecordId recId = iter.next();
				ObjectLocker.getInstance().acquireLock(recId);
				BlockPtr ptr = recId.getPtr();
				RecordBlock rb = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(ptr, colSchemaId, pinnedBlocks);
				IndexKeyType ikt = null;
				rb.readLock();
				
				try {
					TableRecord tr = (TableRecord) CacheObjectMgr.getInstance().getRecord(rb, recId, loadColumns, colSchemaId, pinnedBlocks);
					ikt = buildIndexKey(tr, loadColumns, null);
				} finally {
					CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
					rb.readUnlock();
				}
				
	
				if (idxMeta.getIndex().isUnique()) {
					if (!isUniqueIndexViolation(shards.get(x), recId, ikt, idxMeta, pinnedBlocks)) {
						TransactionId txnId = null;
	//					TransactionId txnId = LogManager.getInstance().startTxn();
						try {
							ikt = new IndexKeyType(ikt.getValue(), recId);
							idxMeta.getIndexTree(shards.get(x)).insert(ikt, pinnedBlocks, txnId);
						} finally {
							LogManager.getInstance().commitTxn(txnId);
							CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
							InProcessIndexQueryIndexMgr.getInstance().done(ikt);
						}
					} else {
						throw new UniqueKeyViolationException();
					}
				}
			}
		}
	}
	
	
	private boolean isUniqueIndexViolation(Shard shard, RecordId recId, IndexKeyType ikt, IndexMetadata idxMeta, Set<BlockPtr> pinnedBlocks) {
		if (InProcessIndexQueryIndexMgr.getInstance().canProcess(ikt)) {
			BTree tree = idxMeta.getIndexTree(shard);
			IndexCompareIndexQuery iciq = new IndexCompareIndexQuery(ikt);
			ResultIterator iter = null;
			try {
				iter = tree.find(iciq, false, pinnedBlocks);
				if (iter.hasNext()) {
					IndexKeyType ikt1 = (IndexKeyType) iter.nextEntry();
					RecordId ikt1RecId = ikt1.getRecordId();
					if (ikt.compareTo(ikt1) == 0 && !ikt1RecId.equals(recId)) {
						InProcessIndexQueryIndexMgr.getInstance().done(ikt);
						return true;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (iter != null) {
					iter.unlock();
				}
			}
		}
		
		return false;
	}
	
	private IndexKeyType buildIndexKey(TableRecord tr, List<ColumnType> loadColumns, RecordId recId) {
		List<DBType> values = new ArrayList<DBType>();
		for (int i = 0; i < loadColumns.size(); i++) {
			values.add(tr.getColumns().get(loadColumns.get(i)));
		}
		return new IndexKeyType(values, recId);
	}
}
