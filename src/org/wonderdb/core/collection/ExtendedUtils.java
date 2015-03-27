package org.wonderdb.core.collection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.impl.PrimaryCacheResourceProvider;
import org.wonderdb.cache.impl.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.impl.SecondaryCacheResourceProvider;
import org.wonderdb.cache.impl.SecondaryCacheResourceProviderFactory;
import org.wonderdb.file.StorageUtils;
import org.wonderdb.freeblock.FreeBlockFactory;
import org.wonderdb.seralizers.block.SerializedBlockImpl;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.Extended;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.record.Record;

public class ExtendedUtils {
	CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	CacheHandler<BlockPtr, List<Record>> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();

	private static ExtendedUtils instance = new ExtendedUtils();
	
	private ExtendedUtils() {
	}
	
	public static ExtendedUtils getInstance() {
		return instance;
	}
	
	public void releaseExtended(Extended extended) {
		Set<Object> pinnedBlocks = new HashSet<Object>();
		try {
			List<BlockPtr> ptrList = extended.getPtrList();
			for (int i = 0; i < ptrList.size(); i++) {
				BlockPtr p = ptrList.get(i);
				CacheEntryPinner.getInstance().pin(p, pinnedBlocks);
				SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(p);
				secondaryResourceProvider.returnResource(serializedBlock);
				primaryResourceProvider.returnResource(p);
				FreeBlockFactory.getInstance().returnBlock(p);
			}
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public List<BlockPtr> extend(byte fileId, Set<Object> pinnedBlocks) {
		long posn = FreeBlockFactory.getInstance().getFreeBlockPosn(fileId);
		BlockPtr ptr = new SingleBlockPtr(fileId, posn);
		CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
		SerializedBlockImpl resource = (SerializedBlockImpl) secondaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		primaryResourceProvider.getResource(ptr, StorageUtils.getInstance().getSmallestBlockCount(ptr));
		secondaryCacheHandler.forceAdd(resource);
		List<BlockPtr> list = new ArrayList<BlockPtr>();
		list.add(ptr);
		return list;
	}
	
//	public void extend(List<BlockPtr> list, byte fileId, int maxBlockSize, int requiredSize, Set<Object> pinnedBlocks) {
//		int noOfBlocks = maxBlockSize/requiredSize;
//		int leftOver = maxBlockSize % requiredSize;
//		int blocksRequired = leftOver > 0 ? noOfBlocks+1 : noOfBlocks;
//		
//		if (blocksRequired == list.size()) {
//			return;
//		}
//		
//		List<BlockPtr> tmpList = new ArrayList<>();
//		if (blocksRequired > list.size()) {
//			for (int i = list.size(); i < blocksRequired; i++) {
//				BlockPtr ptr = extend(fileId, pinnedBlocks);
//				tmpList.add(ptr);
//			}
//			list.addAll(tmpList);
//		} else {
//			for (int i = blocksRequired; i < list.size(); i++) {
//				BlockPtr ptr = list.get(i);
//				FreeBlockFactory.getInstance().returnBlock(ptr);
//				tmpList.add(ptr);
//			}
//			list.removeAll(tmpList);
//		}
//	}	
}
