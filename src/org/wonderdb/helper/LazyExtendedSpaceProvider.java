package org.wonderdb.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.Cacheable;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.InflightFileReader;
import org.wonderdb.cache.impl.PrimaryCacheResourceProvider;
import org.wonderdb.cache.impl.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.core.collection.ExtendedUtils;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.freeblock.FreeBlockFactory;
import org.wonderdb.serialize.DefaultSerializer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.types.BlockPtr;

public class LazyExtendedSpaceProvider {
	
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
//	private static SecondaryCacheResourceProvider cacheResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();


	private static LazyExtendedSpaceProvider instance = new LazyExtendedSpaceProvider();
	private LazyExtendedSpaceProvider() {
	}
	
	public static LazyExtendedSpaceProvider getInstance() {
		return instance;
	}
	
	public ChannelBuffer provideSpaceToRead(List<BlockPtr> extendedPtrs, Set<Object> pinnedBlocks) {
		BlockPtr currentPtr = extendedPtrs.get(0);
		Cacheable<BlockPtr, ChannelBuffer> currentBlock = null;
		List<ChannelBuffer> list = new ArrayList<ChannelBuffer>();
		
		while (true) {
			if (currentPtr == null) {
				break;
			}
			CacheEntryPinner.getInstance().pin(currentPtr, pinnedBlocks); 
			currentBlock = secondaryCacheHandler.get(currentPtr);
			if (currentBlock == null) {
				currentBlock = InflightFileReader.getInstance().getBlock(currentPtr);
			}
			
			primaryResourceProvider.getResource(currentPtr, StorageUtils.getInstance().getSmallestBlockCount(currentPtr));
			ChannelBuffer b = currentBlock.getData();
			b.writerIndex(b.capacity());
			currentPtr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, b, null);
			if (currentPtr != null) {
				extendedPtrs.add(currentPtr);
			}
			
			ChannelBuffer buffer = b.slice(10, b.capacity()-10);
			buffer.clear();
			buffer.writerIndex(buffer.capacity());
			list.add(buffer);
		}
		ChannelBuffer[] bufer = new ChannelBuffer[list.size()];
		ChannelBuffer buf = ChannelBuffers.wrappedBuffer(list.toArray(bufer));
		buf.clear();
		buf.writerIndex(buf.capacity());
		return buf;
	}
	
	public ChannelBuffer provideSpaceToWrite(byte fileId, List<BlockPtr> list, int requiredBlocks, Set<Object> pinnedBlocks) {		
		List<ChannelBuffer> buffers = new ArrayList<ChannelBuffer>();

		Cacheable<BlockPtr, ChannelBuffer> block = null;
		while (list.size() > requiredBlocks) {
			BlockPtr p = list.remove(list.size()-1);
			CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
			secondaryCacheHandler.remove(p);
			primaryResourceProvider.returnResource(p);
			FreeBlockFactory.getInstance().returnBlock(p);
		}
		
		while (requiredBlocks > list.size()) {
			List<BlockPtr> l = ExtendedUtils.getInstance().extend(fileId, pinnedBlocks);
			BlockPtr p = l.get(l.size()-1);
			BlockPtr prevPtr = list.get(list.size()-1);
			if (p.getBlockPosn() < prevPtr.getBlockPosn()) {
				int x = 0;
				x=20;
			}
			CacheEntryPinner.getInstance().pin(prevPtr, pinnedBlocks);
			block = secondaryCacheHandler.get(prevPtr);
			ChannelBuffer buf = block.getData();
			buf.clear();
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, p, buf, null);
			list.add(p);
		}

		BlockPtr p = list.get(list.size()-1);
		CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
		block = secondaryCacheHandler.get(p);
		ChannelBuffer buf = block.getData();
		buf.clear();
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, DefaultSerializer.NULL_BLKPTR, buf, null);
		
		for (int i = 0; i < list.size(); i++) {
			CacheEntryPinner.getInstance().pin(list.get(i), pinnedBlocks);
			block = secondaryCacheHandler.get(list.get(i));
			block.getData().clear();
			ChannelBuffer b = block.getData().slice(10, block.getData().capacity()-10);
			b.clear();
			b.writerIndex(b.capacity());
			buffers.add(b);
		}
		
		if (requiredBlocks > 0) {
			ChannelBuffer[] b = new ChannelBuffer[buffers.size()];
			ChannelBuffer bufer =  ChannelBuffers.wrappedBuffer(buffers.toArray(b));
			bufer.clear();
			return bufer;
		}
		return null;
	}
}
