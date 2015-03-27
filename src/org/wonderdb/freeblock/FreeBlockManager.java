package org.wonderdb.freeblock;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.IndexBranchBlock;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.impl.SecondaryCacheResourceProvider;
import org.wonderdb.cache.impl.SecondaryCacheResourceProviderFactory;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.storage.FileBlockManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.Record;


class FreeBlockMgrNew {
	private static SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	byte fileId;
	int freePosnsSize = 0;
	int lowWatermark;
	int highWatermark;
	AtomicBoolean backendSyncRunning = new AtomicBoolean(false);
	IndexBranchBlock currentBlock = null;
	List<BlockPtr> freePtrList = new ArrayList<BlockPtr>();
	
	FreeBlockMgrNew(byte fileId, int lowWatermarkMultiplier, int highWatermarkMultiplier) {
//		Set<Object> pinnedBlocks = new HashSet<>();
//		try {
//			this.fileId = fileId;
//			BlockPtr ptr = new SingleBlockPtr(fileId, 0);
//			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
//			SerializedBlockImpl sb = (SerializedBlockImpl) secondaryCacheHandler.get(ptr);
//			if (sb == null) {
//				sb = (SerializedBlockImpl) InflightFileReader.getInstance().getBlock(ptr);
//				secondaryCacheHandler.addIfAbsent(sb);
//			}
//			currentBlock = new IndexBranchBlock(ptr);
//			initialize(sb, lowWatermarkMultiplier, highWatermarkMultiplier);
//		} finally {
//			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
//		}
	}
	
	public FreeBlockMgrNew(byte fileId, TransactionId txnId, int lowWatermarkMultiplier, int highWatermarkMultiplier) {
//		Set<Object> pinnedBlocks = new HashSet<>();
//		SerializedBlockImpl sb = null;
//		BlockPtr ptr = new SingleBlockPtr(fileId, 0);
//		this.fileId = fileId;
//		try {
//			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
//			sb = (SerializedBlockImpl) secondaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
//			currentBlock = new IndexBranchBlock(ptr);
//			BlockSerilizer.getInstance().serialize(currentBlock, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), txnId);
//			secondaryCacheHandler.addIfAbsent(sb);
//			initialize(sb, lowWatermarkMultiplier, highWatermarkMultiplier);
//		} finally {
//			LogManager.getInstance().logBlock(txnId, sb);
//			CacheEntryPinner.getInstance().unpin(ptr, pinnedBlocks);
//		}
	}
	
//	private void initialize(SerializedBlockImpl sb, int lowWatermarkMultiplier, int highWatermarkMultiplier) {
//		int size = sb.getData().capacity();
//		freePosnsSize = size/10 - 2;
//		lowWatermark = freePosnsSize * lowWatermarkMultiplier;
//		highWatermark = freePosnsSize * highWatermarkMultiplier;		
//	}
	
	public synchronized void returnBlock(BlockPtr ptr) {
		freePtrList.add(ptr);
		if (freePtrList.size() >= highWatermark && !backendSyncRunning.get()) {
			FreeBlockFactory.getInstance().triggerWrite(this);
		}
	}
		
	public synchronized long getFreePosn() {
//		BlockPtr ptr = null;
//		if (freePtrList.size() > 0) {
//			ptr = freePtrList.get(freePtrList.size()-1);
//		}
//		if (ptr != null && ptr.getBlockPosn() >= 0) {
//			return ptr.getBlockPosn();
//		}
//		
//		if (freePtrList.size() <= lowWatermark && !backendSyncRunning.get()) {
//			FreeBlockFactory.getInstance().triggerRead(this);
//		}
		long posn = FileBlockManager.getInstance().getNextBlock(fileId);
		return posn;
	}

	public void read() {
		if (!backendSyncRunning.compareAndSet(false, true)) {
			return;
		}
		
		Set<Object> pinnedBlocks = new HashSet<Object>();
		SerializedBlockImpl sb = null;
		
		try {
			if (!currentBlock.getData().isEmpty()) {
				CacheEntryPinner.getInstance().pin(currentBlock.getPtr(), pinnedBlocks);
				sb = (SerializedBlockImpl) secondaryCacheHandler.get(currentBlock.getPtr());
				if (sb == null) {
					sb = (SerializedBlockImpl) secondaryResourceProvider.getResource(currentBlock.getPtr(), StorageUtils.getInstance().getSmallestBlockCount(currentBlock.getPtr()));
					secondaryCacheHandler.addIfAbsent(sb);
				}
			} else {
				currentBlock = moveToNonEmptyBlock(pinnedBlocks);
			}
			if (!currentBlock.getData().isEmpty()) {
				synchronized (this) {
					List<Record> list = currentBlock.getData();
					for (int i = 0; i < list.size(); i++) {
						IndexRecord record = (IndexRecord) list.get(i);
						BlockPtr p = (BlockPtr) record.getColumn();
						freePtrList.add(p);
					}
					list.clear();
				}
				BlockSerilizer.getInstance().serialize(currentBlock, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), null);
			}
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			backendSyncRunning.set(false);
		}
	}
	
	private IndexBranchBlock moveToNonEmptyBlock(Set<Object> pinnedBlocks) {		
		BlockPtr ptr = null;
		CacheEntryPinner.getInstance().pin(currentBlock.getPtr(), pinnedBlocks);
		if (!currentBlock.getData().isEmpty()) {
			return currentBlock;
		}
		ptr = new SingleBlockPtr(fileId, 0);
		while (ptr != null) {
			SerializedBlockImpl sb = getSerializedBlock(ptr, pinnedBlocks);
			currentBlock = new IndexBranchBlock(sb.getPtr());
			if (currentBlock.getData().isEmpty()) {
				ptr  = currentBlock.getNext();
			}
		}
		return currentBlock;
	}
	
	private void addPtrList(IndexBranchBlock block, List<BlockPtr> list) {
		IndexRecord record = null;
		for (int i = 0; i < list.size(); i++) {
			BlockPtr ptr = list.get(i);
			record = new IndexRecord();
			record.setColumn(ptr);
		}
		block.getData().add(record);
	}
	
	public void write() {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		
		if (!backendSyncRunning.compareAndSet(false, true)) {
			return;
		}
		List<BlockPtr> writeList = new ArrayList<BlockPtr>();
		
		for (int i = 0; i < freePosnsSize; i++) {
			writeList.add(freePtrList.get(0));
		}
		
		try {
			CacheEntryPinner.getInstance().pin(currentBlock.getPtr(), pinnedBlocks);
			
			if (!currentBlock.getData().isEmpty()) {
				long l = getFreePosn();
				BlockPtr p = new SingleBlockPtr(fileId, l);
				CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
				IndexBranchBlock block = (IndexBranchBlock) BlockManager.getInstance().createBranchBlock(p, pinnedBlocks);
				currentBlock.setNext(p);
				BlockSerilizer.getInstance().serialize(currentBlock, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), null);
				currentBlock = block;
			}
			addPtrList(currentBlock, freePtrList);
			BlockSerilizer.getInstance().serialize(currentBlock, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), null);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			backendSyncRunning.set(false);
		}
	}
	
//	private SerializedBlockImpl getFreeResource(BlockPtr ptr, Set<Object> pinnedBlocks) {
//		SerializedBlockImpl sb = (SerializedBlockImpl) secondaryCacheHandler.get(ptr, pinnedBlocks);
//		if (sb == null) {
//			sb = (SerializedBlockImpl) secondaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
//			secondaryCacheHandler.addIfAbsent(sb, pinnedBlocks);
//		}
//		return sb;
//	}
	
	private SerializedBlockImpl getSerializedBlock(BlockPtr ptr, Set<Object> pinnedBlocks) {
		BlockManager.getInstance().getBlock(ptr, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR), pinnedBlocks);
		SerializedBlockImpl sbCurrentBlock = (SerializedBlockImpl) secondaryCacheHandler.get(ptr);
		return sbCurrentBlock;
	}
//	
//	private class FreeBlock {
//		BlockPtr nextPtr = null;
//		BlockPtr currentPtr = null;
//		List<BlockPtr> ptrList = new ArrayList<>();
//		
//		FreeBlock(SerializedBlockImpl sb) {
//			ChannelBuffer buffer = sb.getData();
//			buffer.clear();
//			buffer.writerIndex(buffer.capacity());
//			while (buffer.readable()) {
//				int size = buffer.readInt();
//				for (int i = 0; i < size; i++) {
//					long p = buffer.readLong();
//					BlockPtr ptr = new SingleBlockPtr(fileId, p);
//					ptrList.add(ptr);
//				}
//				long p = buffer.readLong();
//				if (p >= 0) {
//					nextPtr = new SingleBlockPtr(fileId, p);
//				}
//			}
//		}
//		
//		FreeBlock(BlockPtr currentPtr, BlockPtr nextPtr, List<BlockPtr> ptrList) {
//			this.nextPtr = nextPtr;
//			this.ptrList = ptrList;
//		}
//		
//		private void serialize(SerializedBlockImpl sb) {
//			ChannelBuffer buffer = sb.getData();
//			buffer.clear();
//			
//			for (int i = 0; i < ptrList.size(); i++) {
//				buffer.writeLong(ptrList.get(i).getBlockPosn());
//			}
//			long p = nextPtr == null ? -1 : nextPtr.getBlockPosn();
//			buffer.writeLong(p);
//		}
//		
//		BlockPtr getNextPtr() {
//			return nextPtr;
//		}
//		
//		List<BlockPtr> getPtrList() {
//			return ptrList;
//		}
//		
//		void setNextPtr(BlockPtr p)	{
//			nextPtr = p;
//		}
//		
//		void setPtrList(List<BlockPtr> list) {
//			this.ptrList = list;
//		}
//	}
}

