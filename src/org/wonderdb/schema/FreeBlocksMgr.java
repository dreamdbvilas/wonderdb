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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.index.IndexBlock;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.index.factory.BlockFactory;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.CacheObjectMgr;
import org.wonderdb.cache.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedContinuationBlock;
import org.wonderdb.server.WonderDBServer;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;

public class FreeBlocksMgr {
	private static BlockPtr BOOT_PTR = new SingleBlockPtr((byte) 0, 4*WonderDBServer.DEFAULT_BLOCK_SIZE);
	private static int LOW_WATERMARK = 10;
	private static int HIGH_WATERMARK = 225;
	private static int FREE_BLOCKS_PER_BLOCK = 175;
	
	private BlockPtr head = BOOT_PTR;
	private FreeListWriter writer = new FreeListWriter();
	private FreeListReader reader = new FreeListReader();
	
	ConcurrentMap<Byte, FreeListEntry> freeBlocksMap = new ConcurrentHashMap<Byte, FreeListEntry>();
	
	private static CacheHandler<CacheableList> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	private static CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();

	private static FreeBlocksMgr instance = new FreeBlocksMgr();
	
	public static FreeBlocksMgr getInstance() {
		return instance;
	}
	
	public void addStorage(byte fileId) {
		for (byte i = (byte) freeBlocksMap.size(); i <= fileId; i++) {
			freeBlocksMap.putIfAbsent(i, new FreeListEntry());
		}
	}
	
	public void add(int schemaId, BlockPtr ptr) {
		if (CollectionTailMgr.getInstance().existsInFreeList(schemaId, ptr)) {
			return;
		}
		
		FreeListEntry fle = freeBlocksMap.get(ptr.getFileId());
		fle.queue.add(ptr);
		fle.ai.incrementAndGet();
		if (fle.ai.get() >= HIGH_WATERMARK) {
			List<BlockPtr> list = new ArrayList<BlockPtr>();
			while (fle.ai.get() > LOW_WATERMARK && list.size() < FREE_BLOCKS_PER_BLOCK) {
				BlockPtr p = fle.queue.poll();
				if (p == null) {
					fle.queue.addAll(list);
					fle.ai.addAndGet(list.size());
					list.clear();
					break;
				}
			}
			
			if (list.size() >= FREE_BLOCKS_PER_BLOCK) {
				synchronized (writer.q) {
					writer.q.add(list);
					writer.q.notifyAll();
				}
			}
		}
	}

	public void add (IndexBranchBlock block) {
	}
	
	public BlockPtr get(byte fileId) {
		FreeListEntry fle = freeBlocksMap.get(fileId);
		BlockPtr p = fle.queue.poll();
		if (p != null) {
			fle.ai.decrementAndGet();
			return p;
		}

		if (!fle.isEmpty) {
			synchronized (reader.q) {
				reader.q.notifyAll();
			}
		}
		
		return null;
	}
	
	private synchronized void updateList(List<BlockPtr> list) {
		for (int i = 0; i < list.size(); i++) {
			primaryCacheHandler.remove(list.get(i));
			secondaryCacheHandler.remove(list.get(i));
		}
		
		BlockPtr p = list.remove(list.size()-1);
		
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		TransactionId txnId = null;
		try {
			CacheEntryPinner.getInstance().pin(head, pinnedBlocks);
			txnId = LogManager.getInstance().startTxn();
			IndexBlock b = BlockFactory.getInstance().createIndexBranchBlock(p, pinnedBlocks);
			b.getData().addAll(list);
			SerializedContinuationBlock contBlock = CacheObjectMgr.getInstance().updateIndexBlock((IndexBlock) b, -1, pinnedBlocks);
			LogManager.getInstance().logBlock(txnId, contBlock.getBlocks().get(0));
			SerializedBlock ib = updateTail(b, pinnedBlocks);
			LogManager.getInstance().logBlock(txnId, ib);
			secondaryCacheHandler.changed(contBlock.getBlocks().get(0));
			secondaryCacheHandler.changed(ib);				
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}			
	}

	private synchronized SerializedBlock updateTail(IndexBlock b, Set<BlockPtr> pinnedBlocks) {
		IndexBlock ib = CacheObjectMgr.getInstance().getIndexBlock(head, -1, pinnedBlocks);
		while (ib.getData().size() <= b.getPtr().getFileId()) {
			ib.getData().add(null);
		}
		b.setNext((BlockPtr) ib.getData().get(b.getPtr().getFileId()));
		ib.getData().set(b.getPtr().getFileId(), b.getPtr());
		
		SerializedContinuationBlock contBlock = CacheObjectMgr.getInstance().updateIndexBlock((IndexBlock) ib, -1, pinnedBlocks);
		return contBlock.getBlocks().get(0);
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void getFromFreeList(byte fileId) {
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		IndexBlock ib = CacheObjectMgr.getInstance().getIndexBlock(head, -1, pinnedBlocks);	
		BlockPtr p = (BlockPtr) ib.getData().get(0);
		IndexBlock freePtrsBlock = CacheObjectMgr.getInstance().getIndexBlock(p, -1, pinnedBlocks);
		
		TransactionId txnId = null;
		boolean isEmpty = false;
		try {
			txnId = LogManager.getInstance().startTxn();
			BlockPtr nextPtr = freePtrsBlock.getNext();
			IndexBlock nextBlock = CacheObjectMgr.getInstance().getIndexBlock(nextPtr, -1, pinnedBlocks);
			if (nextBlock == null) {
				isEmpty = true;
			}
			SerializedBlock headBlock = updateTail(nextBlock, pinnedBlocks);
			
			LogManager.getInstance().logBlock(txnId, headBlock);
			
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
		
		FreeListEntry fle = freeBlocksMap.get(fileId);
		fle.queue.addAll((Collection<? extends BlockPtr>) freePtrsBlock.getData());
		fle.ai.addAndGet(freePtrsBlock.getData().size());
		primaryCacheHandler.remove(freePtrsBlock.getPtr());
		secondaryCacheHandler.remove(freePtrsBlock.getPtr());
		fle.queue.add(freePtrsBlock.getPtr());
		fle.ai.incrementAndGet();
		if (isEmpty) {
			fle.isEmpty = true;
		}
	}

	private class FreeListWriter implements Runnable {
		List<List<BlockPtr>> q = new ArrayList<List<BlockPtr>>();
		
		@Override
		public void run() {
			
			while (true) {
				synchronized (q) {
					try {
						q.wait();
					} catch (InterruptedException e) {
					}
					
					List<BlockPtr> list = q.remove(q.size()-1);
					updateList(list);
				}
			}
		}		
	}
	
	private class FreeListReader implements Runnable {
		ConcurrentLinkedQueue<Byte> q = new ConcurrentLinkedQueue<Byte>();
		
		@Override
		public void run() {
			synchronized (q) {
				while (q.size() == 0) {
					try {
						q.wait();
						Byte b = q.poll();
						if (b == null) {
							continue;
						}
						getFromFreeList(b);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}		
	}
	
	
	private static class FreeListEntry {
		ConcurrentLinkedQueue<BlockPtr> queue = new ConcurrentLinkedQueue<BlockPtr>();
		AtomicInteger ai = new AtomicInteger(0);
		boolean isEmpty = true;
	}
}
