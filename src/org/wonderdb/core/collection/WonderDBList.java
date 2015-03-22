package org.wonderdb.core.collection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.Block;
import org.wonderdb.block.BlockEntryPosition;
import org.wonderdb.block.BlockManager;
import org.wonderdb.block.ListBlock;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.impl.SecondaryCacheResourceProvider;
import org.wonderdb.cache.impl.SecondaryCacheResourceProviderFactory;
import org.wonderdb.core.collection.impl.BaseResultIteratorImpl;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.thread.ThreadPoolExecutorWrapper;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.BlockPtrList;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.Extended;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.ObjectListRecord;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;
import org.wonderdb.types.record.TableRecord;

public class WonderDBList {
	CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	CacheHandler<BlockPtr, List<Record>> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
//	PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	
	private static ThreadPoolExecutorWrapper threadPoolExecutor = new ThreadPoolExecutorWrapper(1, 1, 10, 100, "list");

	String listId;
	BlockPtr head = null;
	int concurrentSize = 0;
	BlockPtr tail = null;

	BlockingDeque<BlockPtr> queue = null;
	int lowWatermark = 0;

	int maxBlockSize = -1;
	AtomicBoolean extendingTail = new AtomicBoolean(false);
	AtomicInteger thrownBlocks = new AtomicInteger(0);

	TailExtender tailExtender = null;
	
	private WonderDBList() {
	}
	
	public static void shutdown() {
		threadPoolExecutor.shutdown();
	}
	
	private WonderDBList(String id, BlockPtr head, List<BlockPtr> tailList, int concurrentSize) {
		this.listId = id;
		this.head = head;
		this.tail = tailList.get(tailList.size()-1);
		this.concurrentSize = concurrentSize <= 0 ? 1 : concurrentSize;
		maxBlockSize = StorageUtils.getInstance().getTotalBlockSize(head) - BlockSerilizer.LIST_BLOCK_HEADER - SerializedBlockImpl.HEADER_SIZE;
		tailExtender = new TailExtender(tailList);
	}
	
	public BlockPtr getHead() {
		return head;
	}
	
	public static WonderDBList create(String id, BlockPtr head, int concurrentSize, TransactionId txnId, Set<Object> pinnedBlocks) {
		CacheEntryPinner.getInstance().pin(head, pinnedBlocks);
		Block headBlock = BlockManager.getInstance().createListBlock(head);
		Block tailBlock = BlockManager.getInstance().createListBlock(head.getFileId(), pinnedBlocks);
		headBlock.setNext(tailBlock.getPtr());
		tailBlock.setPrev(head);
		List<BlockPtr> tailList = new ArrayList<>();
		tailList.add(tailBlock.getPtr());
		WonderDBList retList = new WonderDBList(id, head, tailList, concurrentSize);
		retList.updateTail((ListBlock) headBlock, tailList, pinnedBlocks, txnId);	
		
		retList.serializeMinimum(tailBlock, null, txnId);
		return retList;
	}
	
	public BlockPtr getRealHead(Set<Object> pinnedBlocks, TypeMetadata meta) {
		ListBlock headBlock = (ListBlock) BlockManager.getInstance().getBlock(head, meta, pinnedBlocks);
		return headBlock.getNext();
	}
	
	public static WonderDBList load(String id, BlockPtr head, int concurrentSize, TypeMetadata meta, Set<Object> pinnedBlocks) {
		ListBlock headBlock = (ListBlock) BlockManager.getInstance().getBlock(head, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), pinnedBlocks);
		ObjectListRecord record = (ObjectListRecord) headBlock.getData().get(0);
		BlockPtrList list =  (BlockPtrList) record.getColumn();
		WonderDBList retList = new WonderDBList(id, head, list.getPtrList(), concurrentSize);
		return retList;
	}
	
	public ListRecord add(ListRecord record, TransactionId id, TypeMetadata meta, Set<Object> pinnedBlocks) {
		
		ListRecord blockableRecord = record;
		List<Integer> changedColumnIds = null;
		int maxSize = (int) (maxBlockSize * 0.8);
		if (record instanceof TableRecord) {
			blockableRecord = RecordUtils.getInstance().convertToExtended((TableRecord) record, pinnedBlocks, meta, maxSize, 0, head.getFileId());
			changedColumnIds = new ArrayList<>(((TableRecord) blockableRecord).getColumnMap().keySet());
		} else if (record instanceof ObjectListRecord) {
			blockableRecord = (ObjectListRecord) RecordUtils.getInstance().convertToExtended((ObjectRecord) record, pinnedBlocks, meta, maxSize, head.getFileId());
		}
				
		int size = RecordSerializer.getInstance().getRecordSize(blockableRecord, meta);
		ListBlock block = (ListBlock) tailExtender.getBlock(size, pinnedBlocks, meta);
//		block.writeLock();
		try {
			int posn = block.getAndIncMaxPosn();
			RecordId recordId = new RecordId(block.getPtr(), posn);
			blockableRecord.setRecordId(recordId);
			block.getData().add(blockableRecord);

			serializeRecord(blockableRecord, changedColumnIds, pinnedBlocks, id, meta);
			block.adjustResourceCount(blockableRecord.getResourceCount());
		} finally {
			block.writeUnlock();
			tailExtender.returnBlock(block.getPtr());
		}
		return blockableRecord;
	}
	
	public void update(ListRecord oldRecord,ListRecord newRecord, TransactionId txnId, TypeMetadata meta, Set<Object> pinnedBlocks) {
		ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(oldRecord.getRecordId().getPtr(), meta, pinnedBlocks);
		int blockOldSize = BlockSerilizer.getInstance().getBlockSize(block, meta);
		int recordOldSize = RecordSerializer.getInstance().getRecordSize(oldRecord, meta);
		
		List<Integer> changedColumnIds = null;
//		int consumedResources = RecordUtils.getInstance().getConsumedResources(oldRecord);
		block.writeLock();
		try {
			if (oldRecord instanceof ObjectListRecord) {
				int availableSize = maxBlockSize - blockOldSize + recordOldSize;
				RecordUtils.getInstance().convertToExtended((ObjectListRecord) oldRecord, pinnedBlocks, meta, availableSize, head.getFileId());
			} else {
				oldRecord = RecordUtils.getInstance().convertToExtended((TableRecord) oldRecord, (TableRecord) newRecord, 
						pinnedBlocks, meta, blockOldSize, maxBlockSize, head.getFileId());
			}	
			
			int posn = Collections.binarySearch(block.getData(), oldRecord, new RecordComparator());
			if (posn < 0) {
				System.out.println("Issue: trying to update deleted record. Should never happen");
			}
			block.getData().set(posn, oldRecord);
			
//			int newConsumedResources = RecordUtils.getInstance().getConsumedResources(oldRecord);
//			primaryResourceProvider.getResource(block.getPtr(), newConsumedResources-consumedResources);
			if (oldRecord instanceof TableRecord) {
				changedColumnIds = new ArrayList<>(((TableRecord) oldRecord).getColumnMap().keySet());
			}
			serializeRecord(oldRecord, changedColumnIds, pinnedBlocks, txnId, meta);
			block.adjustResourceCount(newRecord.getResourceCount() - oldRecord.getResourceCount());
		} finally {
			block.writeUnlock();
		}
	}
	
	public void deleteRecord(RecordId recordId, TransactionId txnId, TypeMetadata meta, Set<Object> pinnedBlocks) {
		BlockPtr ptr = recordId.getPtr();
		ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(ptr, meta, pinnedBlocks);
		block.writeLock();
		try {
			ListRecord record = new ObjectListRecord(null);
			record.setRecordId(recordId);
			int p = Collections.binarySearch(block.getData(), record, new RecordComparator());
			if (p >= 0) {
				Record r = block.getData().remove(p);
				block.adjustResourceCount(-1*r.getResourceCount());
				RecordUtils.getInstance().releaseRecord(record);
				BlockSerilizer.getInstance().serialize(block, meta, txnId);
			}
		} finally {
			block.writeUnlock();
		}
	}
		
	public ResultIterator iterator(TypeMetadata meta, Set<Object> pinnedBlocks) {
		BlockPtr ptr = getRealHead(pinnedBlocks, meta);
		Block block = BlockManager.getInstance().getBlock(ptr, meta, pinnedBlocks);
		block.readLock();
		BlockEntryPosition bep = new BlockEntryPosition(block, 0);
		return new BaseResultIteratorImpl(bep, false, meta) {

//			@Override
//			public String getSchemaObjectName() {
//				return listId;
//			}
		};
	}
	
	
	private static class RecordComparator implements Comparator<Record> {

		@Override
		public int compare(Record o1, Record o2) {
			ListRecord r1 = (ListRecord) o1;
			ListRecord r2 = (ListRecord) o2;
			int p1 = r1.getRecordId().getPosn();
			int p2 = r2.getRecordId().getPosn();
			return p1 < p2 ? -1 : p1 > p2 ? 1 : 0;
		}
		
	}
	
	private class TailExtender {
		
		private TailExtender(List<BlockPtr> tailList) {
			queue = new LinkedBlockingDeque<>();
			queue.addAll(tailList);
			lowWatermark = (int) 0.5 * concurrentSize;
			
			if (thrownBlocks.get() >= concurrentSize-lowWatermark) {
				if (extendingTail.compareAndSet(false, true)) {
					ExtendTailTask task = new ExtendTailTask(queue.remainingCapacity());
					threadPoolExecutor.asynchrounousExecute(task);
				}
			}
		}
		
		public Block getBlock(int requiredSize, Set<Object> pinnedBlocks, TypeMetadata meta) {
			BlockPtr ptr = null;
			while (true) {
				try {
					ptr = queue.poll(25, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (ptr == null) {
					if (extendingTail.compareAndSet(false, true)) {
						ExtendTailTask task = new ExtendTailTask(concurrentSize-lowWatermark);
						threadPoolExecutor.asynchrounousExecute(task);
					}
				} else {
					Block block = BlockManager.getInstance().getBlock(ptr, meta, pinnedBlocks);
					boolean unlock = false;
					block.writeLock();
					try {
						int size = BlockSerilizer.getInstance().getBlockSize(block, meta);
						if ((maxBlockSize*0.7) > (size + requiredSize)) {
							return block;
						} else {
							thrownBlocks.incrementAndGet();
							unlock = true;
						}
					} finally {
						if (unlock) {
							block.writeUnlock();
						}
					}
				}
			}
		}
	
		public void returnBlock(BlockPtr ptr) {
			queue.addFirst(ptr);
		}
	}
	
	private class ExtendTailTask implements Callable<Boolean> {
		int createAhead = 0;
		
		public ExtendTailTask(int createAhead) {
			this.createAhead = createAhead;
		}
		
		@Override
		public Boolean call() throws Exception {
			TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata(listId);
			TransactionId txnId = LogManager.getInstance().startTxn();
			Set<Object> pinnedBlocks = new HashSet<>();
			List<Block> list = new ArrayList<>();
			try {
				
				for (int i = 0; i < createAhead; i++) {
					Block b = BlockManager.getInstance().createListBlock(head.getFileId(), pinnedBlocks);
					list.add(b);
				}
				
				list.get(0).setPrev(tail);
				for (int i = 1; i < list.size(); i++) {
					Block currentBlock = list.get(i);
					Block prevBlock = list.get(i-1);
					prevBlock.setNext(currentBlock.getPtr());
					currentBlock.setPrev(prevBlock.getPtr());
				}
				for (int i = 0; i < list.size(); i++) {
					Block currentBlock = list.get(i);
					CacheEntryPinner.getInstance().pin(currentBlock.getPtr(), pinnedBlocks);
					BlockSerilizer.getInstance().serialize(currentBlock, meta, txnId);
					secondaryCacheHandler.changed(currentBlock.getPtr());
				}
			} finally {
				LogManager.getInstance().commitTxn(txnId);
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}

			pinnedBlocks.clear();
			List<BlockPtr> tailList = new ArrayList<>(list.size());
			for (int i = 0; i < list.size(); i++) {
				tailList.add(list.get(i).getPtr());
			}
			Block tailBlock = null;
			txnId = LogManager.getInstance().startTxn();
			try {
				tailBlock = BlockManager.getInstance().getBlock(tail, meta, pinnedBlocks);
				tailBlock.writeLock();
				tailBlock.setNext(list.get(0).getPtr());
				SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(tail);
				BlockSerilizer.getInstance().serialize(tailBlock, meta, txnId);
				secondaryCacheHandler.changed(tail);
				updateTail(head, tailList, pinnedBlocks, txnId);
				
				LogManager.getInstance().logBlock(txnId, serializedBlock);
				
			} finally {
				LogManager.getInstance().commitTxn(txnId);
				tailBlock.writeUnlock();
				CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			}
			
			tail = list.get(list.size()-1).getPtr();
			thrownBlocks.set(0);
			for (int i = 0; i < list.size(); i++) {
				queue.add(list.get(i).getPtr());
			}
			extendingTail.set(false);
			return true;
		}
	}
	
	private void updateTail(ListBlock block, List<BlockPtr> tailList, Set<Object> pinnedBlocks, TransactionId txnId) {
		BlockPtrList list = new BlockPtrList(tailList);
		List<Record> records = block.getData();
		ObjectListRecord record = null;
		if (records.size() >0) {
			record = (ObjectListRecord) records.get(0);
			record.setColumn(list);
		} else {
			record = new ObjectListRecord(list);
			record.setRecordId(new RecordId(head, block.getAndIncMaxPosn()));
			records.add(record);
		}
		serializeMinimum(block, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), txnId);
		secondaryCacheHandler.changed(block.getPtr());
		
	}

	private void updateTail(BlockPtr ptr, List<BlockPtr> tailList, Set<Object> pinnedBlocks, TransactionId txnId) {
		ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(ptr, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), pinnedBlocks);
		updateTail(block, tailList, pinnedBlocks, txnId);
	}

	private void serializeMinimum(Block block, TypeMetadata meta,TransactionId txnId) {
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(block.getPtr());
		BlockSerilizer.getInstance().serialize(block, meta, txnId);
		LogManager.getInstance().logBlock(txnId, serializedBlock);
	}
	
	private void serializeRecord(ListRecord record, List<Integer> changedColumnIds, Set<Object> pinnedBlocks, TransactionId txnId, TypeMetadata meta) {
		Block block = BlockManager.getInstance().getBlock(record.getRecordId().getPtr(), meta, pinnedBlocks);
		BlockSerilizer.getInstance().serialize(block, meta, txnId);
		if (record instanceof ExtendedTableRecord) {
			RecordSerializer.getInstance().serializeExtended(head.getFileId(), (ExtendedTableRecord) record, maxBlockSize, meta, pinnedBlocks);
			List<BlockPtr> list = ((Extended) record).getPtrList();
			for (int i = 0; i < list.size(); i++) {
				BlockPtr p = list.get(i);
				CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
				SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(p);
				secondaryCacheHandler.changed(p);
				LogManager.getInstance().logBlock(txnId, serializedBlock);
			}
		}
			
		if (record instanceof TableRecord) {
			Map<Integer, DBType> map = ((TableRecord) record).getColumnMap();
			Iterator<Integer> colIdIter = changedColumnIds.iterator();
			while (colIdIter.hasNext()) {
				int colId = colIdIter.next();
				DBType column = map.get(colId);
				if (column instanceof Extended) {
					int type = ((TableRecordMetadata)meta).getColumnIdTypeMap().get(colId);
					ColumnSerializer.getInstance().serializeExtended(head.getFileId(), (ExtendedColumn) column, maxBlockSize, 
							new ColumnSerializerMetadata(type), pinnedBlocks);
					List<BlockPtr> list = ((Extended) column).getPtrList();
					for (int i = 0; i < list.size(); i++) {
						BlockPtr p = list.get(i);
						CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
						SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(p);
						secondaryCacheHandler.changed(p);
						LogManager.getInstance().logBlock(txnId, serializedBlock);
					}						
				}
			}
		} else if (record instanceof ObjectListRecord) {
			DBType column = ((ObjectListRecord) record).getColumn();
			if (column instanceof Extended) {
				ColumnSerializer.getInstance().serializeExtended(head.getFileId(), (ExtendedColumn) column, maxBlockSize, meta, pinnedBlocks);
				List<BlockPtr> list = ((Extended) column).getPtrList();
				for (int i = 0; i < list.size(); i++) {
					BlockPtr p = list.get(i);
					CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
					SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(p);
					secondaryCacheHandler.changed(p);
					LogManager.getInstance().logBlock(txnId, serializedBlock);
				}
			}
		}
	}
}
