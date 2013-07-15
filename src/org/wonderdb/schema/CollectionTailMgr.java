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
package org.wonderdb.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.impl.base.SingleBlockPtrList;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.factory.BlockFactory;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cache.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedRecordLeafBlock;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.metadata.impl.SerializedCollectionMetadata;


public class CollectionTailMgr {
	private static int LOW_WATER_MARK = 3;
	private static int HIGH_WATER_MARK = 5;
	private CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	CacheHandler<CacheableList> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();

	Thread tailExtendThread = null;
	
	private ConcurrentMap<ShardKey, CollectionTailData> collectionTailPtrMap = new ConcurrentHashMap<ShardKey, CollectionTailMgr.CollectionTailData>();
	private BlockingQueue<ShardKey> extendTail = new ArrayBlockingQueue<ShardKey>(1000);
	
	private static CollectionTailMgr instance = new CollectionTailMgr();
	private CollectionTailMgr() {
		TailCreationThread tct = new TailCreationThread();
		tailExtendThread = new Thread(tct, "TailCreationThread");
		tailExtendThread.start();
	}
	
	public static CollectionTailMgr getInstance() {
		return instance;
	}
	
	public void shutdown() {
		tailExtendThread.interrupt();
	}
	
	public CollectionMetadata initializeCollectionTailData(Shard shard) {
		CollectionTailData ctd = new CollectionTailData();
		ctd.tailPtr = null;
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(shard.getSchemaId());
		
		ShardKey sk = new ShardKey(shard.getSchemaId(), shard);
		collectionTailPtrMap.putIfAbsent(sk, ctd);
		
		CollectionMetadata mColMeta = SchemaMetadata.getInstance().getCollectionMetadata("metaCollection");
		TransactionId txnId = null;
		txnId = LogManager.getInstance().startTxn();
		byte fileId = FileBlockManager.getInstance().getId(shard);
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		@SuppressWarnings("unused")
		long posn = FileBlockManager.getInstance().getNextBlock(shard);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		@SuppressWarnings("unused")
		IndexBlock ib = BlockFactory.getInstance().createIndexBranchBlock(p, pinnedBlocks);
		try {
			SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
			scm.setFixedColumns(colMeta.getName(), colMeta.getSchemaId(), false);
			BlockPtr ptr = new SingleBlockPtr((byte) -1, -1);
			scm.updateHead(shard, ptr, txnId);
			scm.updateTail(shard, ctd.allTails, txnId);
			scm.addColumns(colMeta.getCollectionColumns());
			TableRecord record = new TableRecord(scm.getColumns());
			Shard metaShard = new Shard(1, "", "");
			mColMeta.getRecordList(metaShard).add(record, null, txnId);
			colMeta.setRecordId(record.getRecordId());
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		return colMeta;
	}
	
	public void updateShardHeadTail(Shard shard) {
		CollectionTailData ctd = new CollectionTailData();
		ctd.tailPtr = null;
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(shard.getSchemaId());
		
		ShardKey sk = new ShardKey(shard.getSchemaId(), shard);
		collectionTailPtrMap.putIfAbsent(sk, ctd);
		
		TransactionId txnId = null;
		txnId = LogManager.getInstance().startTxn();
		byte fileId = FileBlockManager.getInstance().getId(shard);
		BlockPtr p = new SingleBlockPtr(fileId, 0);
		@SuppressWarnings("unused")
		long posn = FileBlockManager.getInstance().getNextBlock(shard);
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		@SuppressWarnings("unused")
		IndexBlock ib = BlockFactory.getInstance().createIndexBranchBlock(p, pinnedBlocks);
		try {
			SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
			scm.setFixedColumns(colMeta.getName(), colMeta.getSchemaId(), false);
			BlockPtr ptr = new SingleBlockPtr((byte) -1, -1);
			scm.updateHead(shard, ptr, txnId);
			scm.updateTail(shard, ctd.allTails, txnId);
			LogManager.getInstance().commitTxn(txnId);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public void bootstrap(int schemaId, BlockPtr tailPtr) {
		CollectionTailData ctd = new CollectionTailData();
		ctd.tailPtr = tailPtr;
		ctd.allTails.add(tailPtr);
		ctd.freePtrs.add(tailPtr);
		Shard shard = new Shard(schemaId, "", "");
		ShardKey sk = new ShardKey(schemaId, shard);
		collectionTailPtrMap.putIfAbsent(sk, ctd);
	}
	
	public void initializeCollectionData(int schemaId, Shard shard, SingleBlockPtrList list) {
		CollectionTailData ctd = new CollectionTailData();
		ctd.allTails = list;
		ctd.freePtrs = new LinkedBlockingDeque<BlockPtr>();
		ctd.freePtrs.addAll(list);
		ctd.tailPtr = ctd.allTails.size() > 0 ? ctd.allTails.get(ctd.allTails.size()-1) : null;
		ShardKey sk = new ShardKey(schemaId, shard);
		collectionTailPtrMap.putIfAbsent(sk, ctd);
		
	}
	
	public void bootstrapSerialize(int schemaId) {
		ShardKey sk = new ShardKey(schemaId, new Shard(schemaId, "", ""));
		CollectionTailData ctd = collectionTailPtrMap.get(sk);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata("metaCollection");
		CollectionMetadata srcColMeta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
		BlockPtr metaPtr = ctd.tailPtr;
		SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
		
		scm.setFixedColumns(srcColMeta.getName(), schemaId, false);
		TransactionId txnId = LogManager.getInstance().startTxn();
		scm.updateHead(new Shard(schemaId, "", ""), metaPtr, txnId);
		scm.updateTail(new Shard(schemaId, "", ""), ctd.allTails, txnId);
		TableRecord record = new TableRecord(scm.getColumns());
		colMeta.getRecordList(sk.shard).add(record, null, null);
		LogManager.getInstance().commitTxn(txnId);
//		updateTailList(schemaId);
	}
	
	public RecordBlock getBlock(int schemaId, Shard shard, Set<BlockPtr> pinnedBlocks) {
		ShardKey sk = new ShardKey(schemaId, shard);
		CollectionTailData ctd = collectionTailPtrMap.get(sk);
		BlockPtr ptr = null;
		try {
			if ((schemaId < 3 && ctd.freePtrs.size() == 0) || ( schemaId >= 3 && ctd.freePtrs.size() < LOW_WATER_MARK)) {
				extendTail.put(sk);
			}
			ptr = ctd.freePtrs.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		return (RecordBlock) CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(ptr, schemaId, pinnedBlocks);
	}
	
	public boolean existsInFreeList(int schemaId, BlockPtr ptr) {
		List<CollectionTailData> ctdList = new ArrayList<CollectionTailData>(collectionTailPtrMap.values());
		for (int i = 0; i < ctdList.size(); i++) {
			CollectionTailData ctd = ctdList.get(i);
			if (ctd != null && ctd.freePtrs.contains(ptr)) {
				return true;
			}
		}
		return false;
	}
	
	public void returnTailPtr(int schemaId, Shard shard, BlockPtr ptr) {
		ShardKey sk = new ShardKey(schemaId, shard);
		CollectionTailData ctd = collectionTailPtrMap.get(sk);
		ctd.freePtrs.push(ptr);
	}
	
	private static class ShardKey {
		Shard shard = null;
		int schemaId = -1;
		
		ShardKey(int schemaId, Shard shard) {
			this.schemaId = schemaId;
			if (shard == null) {
				throw new RuntimeException("Invalid shard");
			}
			this.shard = shard;
		}
		
		public int hashCode() {
			return schemaId;
		}
		
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			
			if (o instanceof ShardKey) {
				return ((ShardKey) o).shard.equals(this.shard); 
			}
			
			return false;
		}
	}
	
	private class CollectionTailData {
		BlockingDeque<BlockPtr> freePtrs = new LinkedBlockingDeque<BlockPtr>();
		SingleBlockPtrList allTails = new SingleBlockPtrList();
		BlockPtr tailPtr = null;
	}
	
	private class TailCreationThread implements Runnable {

		@Override
		public void run() {
			ShardKey sk = null;
			while (true) {
				try {
					sk = extendTail.take();
				} catch (InterruptedException e) {
					return;
				}
				
				updateTailList(sk);
			}
		}		
	}

	private void updateTailList(ShardKey sk) {
		CollectionTailData ctd = collectionTailPtrMap.get(sk);
		if (ctd == null) {
			CollectionTailData newCTD = new CollectionTailData();
			ctd = collectionTailPtrMap.putIfAbsent(sk, newCTD);
			if (ctd == null) {
				ctd = newCTD;
			}
		}
		
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		int currentSize = ctd.freePtrs.size();
		SerializedBlock newTailSb = null;
		RecordBlock newTailRb = null;
		BlockPtr prevTailPtr = ctd.tailPtr;
		List<BlockPtr> tmpPtrList = new ArrayList<BlockPtr>();
		boolean firstTime = false;
		if (prevTailPtr == null) {
			firstTime = true;
		}
		
		if (currentSize > LOW_WATER_MARK) {
			return;
		}
		
		ctd.allTails.clear();
		try {
			for (;currentSize < HIGH_WATER_MARK; currentSize++) {
				newTailRb = BlockFactory.getInstance().createRecordBlock(sk.schemaId, sk.shard, pinnedBlocks);
				ctd.allTails.add(newTailRb.getPtr());
				tmpPtrList.add(newTailRb.getPtr());
				
				newTailSb = (SerializedBlock) secondaryCacheHandler.get(newTailRb.getPtr());
				SerializedRecordLeafBlock newTailSrlb = new SerializedRecordLeafBlock(newTailSb, false);
	
				newTailSrlb.setNextPtr(null);
				newTailRb.setNext(null);
				ctd.tailPtr = newTailRb.getPtr();
				
				if (prevTailPtr != null) {
					RecordBlock prevTailRb = CacheObjectMgr.getInstance().getRecordBlockWithHeaderLoaded(prevTailPtr, sk.schemaId, pinnedBlocks);
					try {
						prevTailRb.writeLock();
						SerializedBlock prevTailSb = (SerializedBlock) secondaryCacheHandler.get(prevTailPtr);
						SerializedRecordLeafBlock prevTailSrlb = new SerializedRecordLeafBlock(prevTailSb, false);
						prevTailRb.setNext(newTailRb.getPtr());
						newTailRb.setPrev(prevTailPtr);
						newTailRb.setNext(null);
						prevTailSrlb.setNextPtr(newTailRb.getPtr());
						newTailSrlb.setPrevPtr(prevTailPtr);
						secondaryCacheHandler.changed(newTailRb.getPtr());
						secondaryCacheHandler.changed(prevTailPtr);
					} finally {
						prevTailRb.writeUnlock();
					}
				} 
				prevTailPtr = newTailSb.getPtr();
			}
			
//			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(sk.schemaId);
//			Map<String, DBType> map = new HashMap<String, DBType>();
//			if (firstTime) {
//				map.put("headPtr", ctd.allTails.get(0));
//			}
//			map.put("tailPtr", ctd.allTails);
			TransactionId txnId = LogManager.getInstance().startTxn();
			try {
				SerializedCollectionMetadata scm = new SerializedCollectionMetadata();
				if (firstTime) {
					scm.updateHead(sk.shard, ctd.allTails.get(0), txnId);
				}
				scm.updateTail(sk.shard, ctd.allTails, txnId);
//				TableRecordManager.getInstance().updateTableRecord("metaCollection", map, colMeta.getRecordId(), null, txnId);
//			} catch (InvalidCollectionNameException e) {
//				e.printStackTrace();
				LogManager.getInstance().commitTxn(txnId);
			} finally {
			}
			ctd.freePtrs.addAll(tmpPtrList);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
}
