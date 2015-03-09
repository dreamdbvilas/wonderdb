package org.wonderdb.serialize.block;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.Block;
import org.wonderdb.block.IndexBranchBlock;
import org.wonderdb.block.IndexLeafBlock;
import org.wonderdb.block.ListBlock;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.record.ObjectRecordSerializer;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.serialize.record.TableRecordSerializer;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TableRecordMetadata;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.ObjectRecord;
import org.wonderdb.types.record.Record;
import org.wonderdb.types.record.TableRecord;

public class BlockSerilizer  {
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();

	private static final int BLOCK_HEADER = SerializedBlockImpl.HEADER_SIZE + 
			1 /* block header */ + 
			(1 + Long.BYTES) /* prev pointer */ + 
			(1 + Long.BYTES) /* next pointer */ + 
			Integer.BYTES /* reserved */ +
			Integer.BYTES /* record list size */;
	
	public static final int INDEX_BLOCK_HEADER = BLOCK_HEADER + 
			(1 + Long.BYTES) /* parent pointer */;
	
	public static final int LIST_BLOCK_HEADER = BLOCK_HEADER + 
			Integer.BYTES /* max posn */;
	
	private static BlockSerilizer instance = new BlockSerilizer();
	
	private BlockSerilizer() {
	}
	
	public static BlockSerilizer getInstance() {
		return instance;
	}
	
	public void serialize(Block block, TypeMetadata meta, TransactionId id) {
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(block.getPtr());
		serializedBlock.setLastAccessTime(block.getLastAccessTime());
		ChannelBuffer buffer = serializedBlock.getData();
		buffer.clear();
		BlockHeader header = getBlockHeader(block);
		BlockHeaderSerializer.getInstance().serialize(header, buffer);
		
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, block.getNext(), buffer, null);
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, block.getPrev(), buffer, null);
		buffer.writeInt(0); //reserved
		
		if (header.isIndexBlock() || header.isIndexBranchBlock()) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, block.getParent(), buffer, meta);
		} else {
			int maxPosn = ((ListBlock) block).getMaxPosn();
			buffer.writeInt(maxPosn);
		}

		List<Record> list = block.getData();
		buffer.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			TypeMetadata newMeta = meta;
			Record record = list.get(i);
			if (record instanceof ListRecord) {
				RecordId recordId = ((ListRecord) record).getRecordId();
				buffer.writeInt(recordId.getPosn());
			}
			RecordSerializer.getInstance().serializeMinimum(record, buffer, newMeta);
		}
		secondaryCacheHandler.changed(block.getPtr());
		LogManager.getInstance().logBlock(id, serializedBlock);
	}
	
	public Block getBlock(SerializedBlockImpl serializedBlock, TypeMetadata meta) {
		BlockPtr ptr = serializedBlock.getPtr();

		ChannelBuffer buffer = serializedBlock.getData();
		
		BlockHeader header = BlockHeaderSerializer.getInstance().getHeader(buffer);
		
		BlockPtr nextPtr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);
		BlockPtr prevPtr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);
		int reserved = buffer.readInt();
		BlockPtr parentPtr = null;
		
		TypeMetadata newMeta = meta;
		
		Block block = null;
		int maxPosn = -1;
		if (header.isIndexBlock() || header.isIndexBranchBlock()) {
			parentPtr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);			
		} else {
			maxPosn = buffer.readInt();
		}
		
		if (header.isIndexBranchBlock()) {
			block = new IndexBranchBlock(ptr);
			block.setParent(parentPtr);
		} else if (header.isIndexBlock()) {
			block = new IndexLeafBlock(ptr);
			block.setParent(parentPtr);
		} else {
			block = new ListBlock(ptr);
			((ListBlock) block).setMaxPosn(maxPosn);
		}
		
		block.setNext(nextPtr);
		block.setPrev(prevPtr);
		block.setLastAccessTime(System.currentTimeMillis());
		
		int size = buffer.readInt();
		List<Record> list = new ArrayList<Record>(size);
		block.setData(list);
		for (int i = 0; i < size; i++) {
			Record record = null;
			if (header.isIndexBranchBlock()) {
				newMeta = new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR);
				record = ObjectRecordSerializer.getInstance().readMinimum(header, buffer, newMeta);
			} else if (header.isIndexBlock()) {
				record = ObjectRecordSerializer.getInstance().readMinimum(header, buffer, meta);
			} else if (meta instanceof TableRecordMetadata){
				int posn = buffer.readInt();
				record = TableRecordSerializer.getInstance().readMinimum(buffer, meta);
				((ListRecord) record).setRecordId(new RecordId(ptr, posn));
			} else {
				int posn = buffer.readInt();
				record = ObjectRecordSerializer.getInstance().readMinimum(header, buffer, meta);				
				((ListRecord) record).setRecordId(new RecordId(ptr, posn));
			}
			list.add(record);
		}
		return block;
	}
	
	public int getBlockSize(Block block, TypeMetadata meta) {
		int size = 0;
		if (block instanceof ListBlock) {
			size = size + LIST_BLOCK_HEADER; 
		} else {
			size = size + INDEX_BLOCK_HEADER;
		}
		List<Record> list = block.getData();
		for (int i = 0; i < list.size(); i++) {
			Record record = list.get(i);
			if (record instanceof ExtendedTableRecord) {
				size = size + 9;
			} else if (record instanceof ObjectRecord) {
				size = size + ObjectRecordSerializer.getInstance().getObjectSize(record, meta);
			} else if (record instanceof TableRecord) {
				size = size + TableRecordSerializer.getInstance().getObjectSize(record, meta);
			}
		}
		return size;
	}

	private BlockHeader getBlockHeader(Block block) {
		BlockHeader header = new BlockHeader();
		if (block instanceof IndexBranchBlock) {
			header.setIndexBranchBlock(true);
		} 
		
		if (block instanceof IndexLeafBlock) {
			header.setIndexBlock(true);
		}

		return header;
	}
}
