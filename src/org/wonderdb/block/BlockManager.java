package org.wonderdb.block;

import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.InflightFileReader;
import org.wonderdb.cache.impl.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.impl.PrimaryCacheResourceProvider;
import org.wonderdb.cache.impl.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.impl.SecondaryCacheResourceProvider;
import org.wonderdb.cache.impl.SecondaryCacheResourceProviderFactory;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.freeblock.FreeBlockFactory;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.serialize.block.BlockSerilizer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.Record;


public class BlockManager {
	CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	CacheHandler<BlockPtr, List<Record>> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();

	private static BlockManager instance = new BlockManager();
	
	private BlockManager() {
	}
	
	public static BlockManager getInstance() {
		return instance;
	}
	
	public Block getBlock(BlockPtr ptr, TypeMetadata meta, Set<Object> pinnedBlocks) {
		if (ptr == null || ptr.getFileId() < 0) {
			return null;
		}
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		Block block = (Block) primaryCacheHandler.get(ptr);
		if (block == null) {
			SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(ptr);
			if (serializedBlock == null) {
				serializedBlock = (SerializedBlockImpl) InflightFileReader.getInstance().getBlock(ptr);
				block = BlockSerilizer.getInstance().getBlock(serializedBlock, meta);
				Block b1 = (Block) primaryCacheHandler.addIfAbsent(block);
				if (b1 == block) {
					primaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
				}
			}
		}
		return block;
	}
	
	public Block createListBlock(byte fileId, Set<Object> pinnedBlocks) {
		SerializedBlockImpl serializedBlock = createSpace(fileId, pinnedBlocks);
		ListBlock block = new ListBlock(serializedBlock.getPtr());
		primaryCacheHandler.forceAdd(block);
		return block;
	}
	
	public Block createListBlock(BlockPtr head) {
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryResourceProvider.getResource(head, StorageUtils.getInstance().getSmallestBlockCount(head));
		secondaryCacheHandler.forceAdd(serializedBlock);
		primaryResourceProvider.getResource(head, StorageUtils.getInstance().getSmallestBlockCount(head));
		ListBlock block = new ListBlock(head);
		primaryCacheHandler.forceAdd(block);
		return block;
	}
	
	public Block createIndexBlock(byte fileId, Set<Object> pinnedBlocks) {
		SerializedBlockImpl serializedBlock = createSpace(fileId, pinnedBlocks);
		IndexLeafBlock block = new IndexLeafBlock(serializedBlock.getPtr());
		primaryCacheHandler.forceAdd(block);
		return block;
	}
	
	public Block createBranchBlock(byte fileId, Set<Object> pinnedBlocks) {
		SerializedBlockImpl serializedBlock = createSpace(fileId, pinnedBlocks);
		IndexBranchBlock block = new IndexBranchBlock(serializedBlock.getPtr());
		primaryCacheHandler.forceAdd(block);
		return block;		
	}
	
	public Block createBranchBlock(BlockPtr ptr, Set<Object> pinnedBlocks) {
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		secondaryCacheHandler.forceAdd(serializedBlock);
		primaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		IndexBranchBlock block = new IndexBranchBlock(ptr);
		primaryCacheHandler.forceAdd(block);
		return block;
	}
	
	public Block createIndexLefBlock(BlockPtr ptr, Set<Object> pinnedBlocks) {
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		secondaryCacheHandler.forceAdd(serializedBlock);
		primaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		IndexLeafBlock block = new IndexLeafBlock(ptr);
		primaryCacheHandler.forceAdd(block);
		return block;
	}
	
	private SerializedBlockImpl createSpace(byte fileId, Set<Object> pinnedBlocks) {
		long posn = FreeBlockFactory.getInstance().getFreeBlockPosn(fileId);
		BlockPtr ptr = new SingleBlockPtr(fileId, posn);
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		secondaryCacheHandler.forceAdd(serializedBlock);
		primaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		return serializedBlock;
	}	
}
