package org.wonderdb.server;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cache.CacheUsage;
import org.wonderdb.cache.Pinner;
import org.wonderdb.cache.impl.BaseCacheHandler;
import org.wonderdb.cache.impl.CacheBean;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.CacheLock;
import org.wonderdb.cache.impl.CacheState;
import org.wonderdb.cache.impl.CacheWriter;
import org.wonderdb.cache.impl.MemoryCacheMap;
import org.wonderdb.cache.impl.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.impl.PrimaryCacheResourceProvider;
import org.wonderdb.cache.impl.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.impl.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.impl.SecondaryCacheResourceProvider;
import org.wonderdb.cache.impl.SecondaryCacheResourceProviderFactory;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.file.FileCacheWriter;
import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.metadata.StorageMetadata;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.FileBlockEntry;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.record.Record;

public class WonderDBCacheService {
	private static WonderDBCacheService instance = new WonderDBCacheService();
	
	private WonderDBCacheService() {
	}

	public static WonderDBCacheService getInstance() {
		return instance;
	}
	
	static CacheBean primaryCacheBean = new CacheBean();
	static CacheState primaryCacheState = new CacheState();
	static MemoryCacheMap<BlockPtr, List<Record>> primaryCacheMap = new MemoryCacheMap<BlockPtr, List<Record>>(1000, 5, false);
	static CacheLock cacheLock = new CacheLock();
	static CacheBean secondaryCacheBean = new CacheBean();
	static CacheState secondaryCacheState = new CacheState();
	static MemoryCacheMap<BlockPtr, ChannelBuffer> secondaryCacheMap = new MemoryCacheMap<BlockPtr, ChannelBuffer>(5000, 5, true);
	static CacheHandler<BlockPtr, List<Record>> primaryCacheHandler = null;
	static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = null;
	public static CacheWriter<BlockPtr, ChannelBuffer> writer = null;

	public void init(String propertyFile) throws Exception {
    	WonderDBPropertyManager.getInstance().init(propertyFile);
    	
		primaryCacheBean.setCleanupHighWaterMark(WonderDBPropertyManager.getInstance().getPrimaryCacheHighWatermark()); // 1000
		primaryCacheBean.setCleanupLowWaterMark(WonderDBPropertyManager.getInstance().getPrimaryCacheLowWatermark()); // 999
		primaryCacheBean.setMaxSize(WonderDBPropertyManager.getInstance().getPrimaryCacheMaxSize()); // 1000
		PrimaryCacheResourceProvider primaryProvider = new PrimaryCacheResourceProvider(primaryCacheBean, primaryCacheState, cacheLock);
		PrimaryCacheResourceProviderFactory.getInstance().setResourceProvider(primaryProvider);
		primaryCacheHandler = new BaseCacheHandler<BlockPtr, List<Record>>(primaryCacheMap, primaryCacheBean, primaryCacheState, 
				cacheLock, primaryProvider, false);
		PrimaryCacheHandlerFactory.getInstance().setCacheHandler(primaryCacheHandler);
		
		writer = new CacheWriter<BlockPtr, ChannelBuffer>(secondaryCacheMap, 1000, new FileCacheWriter());
		writer.start();

		secondaryCacheBean.setCleanupHighWaterMark(WonderDBPropertyManager.getInstance().getSecondaryCacheHighWatermark()); // 1475
		secondaryCacheBean.setCleanupLowWaterMark(WonderDBPropertyManager.getInstance().getSecondaryCacheLowWatermark()); // 1450
		secondaryCacheBean.setMaxSize(WonderDBPropertyManager.getInstance().getSecondaryCacheMaxSize()); // 1500
		CacheLock secondaryCacheLock = new CacheLock();
		SecondaryCacheResourceProvider secondaryProvider = new SecondaryCacheResourceProvider(null, secondaryCacheBean, secondaryCacheState, 
				secondaryCacheLock, WonderDBPropertyManager.getInstance().getSecondaryCacheMaxSize(), WonderDBPropertyManager.getInstance().getDefaultBlockSize(), writer);
		SecondaryCacheResourceProviderFactory.getInstance().setResourceProvider(secondaryProvider);
		secondaryCacheHandler = new BaseCacheHandler<BlockPtr, ChannelBuffer>(secondaryCacheMap, 
				secondaryCacheBean, secondaryCacheState, secondaryCacheLock, secondaryProvider, true);
		SecondaryCacheHandlerFactory.getInstance().setCacheHandler(secondaryCacheHandler);

		MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = new ObjectName("PrimaryCacheState:type=CacheUsage");
		beanServer.registerMBean(new CacheUsage(primaryCacheState), name);
		name = new ObjectName("ScondaryCacheState:type=CacheUsage");
		beanServer.registerMBean(new CacheUsage(secondaryCacheState), name);
		name = new ObjectName("CacahePinner:type=Pinner");
		beanServer.registerMBean(new Pinner(), name);

		String systemFile = WonderDBPropertyManager.getInstance().getSystemFile();
		File file = new File(systemFile);
		if (file.exists()) {
			StorageMetadata.getInstance().init(false);
			SchemaMetadata.getInstance().init(false);
			

		} else {
			StorageMetadata.getInstance().init(true);
			SchemaMetadata.getInstance().init(true);
			
			String cacheStorage = WonderDBPropertyManager.getInstance().getCacheStorage();
			if (cacheStorage != null) {
				FileBlockEntry fbe = new FileBlockEntry();
				fbe.setBlockSize(WonderDBPropertyManager.getInstance().getDefaultBlockSize());
				fbe.setFileName(cacheStorage);
				StorageMetadata.getInstance().add(fbe);
			}
			
			String cacheIndexStorage = WonderDBPropertyManager.getInstance().getCacheIndexStorage();
			if (cacheIndexStorage != null) {
				FileBlockEntry fbe = new FileBlockEntry();
				fbe.setBlockSize(WonderDBPropertyManager.getInstance().getDefaultBlockSize());
				fbe.setFileName(cacheIndexStorage);
				StorageMetadata.getInstance().add(fbe);				
			}
			
			List<ColumnNameMeta> columns = new ArrayList<ColumnNameMeta>();
			ColumnNameMeta cnm = new ColumnNameMeta();
			cnm.setCollectioName("cache");
			cnm.setColumnName("key");
			cnm.setColumnType(SerializerManager.BYTE_ARRAY_TYPE);
			cnm.setCoulmnId(0);
			
			columns.add(cnm);
			cnm = new ColumnNameMeta();
			cnm.setCollectioName("cache");
			cnm.setColumnName("value");
			cnm.setColumnType(SerializerManager.BYTE_ARRAY_TYPE);
			cnm.setCoulmnId(1);
			columns.add(cnm);
			
//			SchemaMetadata.getInstance().createNewCollection("cache", "/data/cache.data", columns, 10);
			SchemaMetadata.getInstance().createNewCollection("cache", cacheStorage, columns, 10);
			
			IndexNameMeta inm = new IndexNameMeta();
			inm.setIndexName("cacheIndex");
			inm.setAscending(true);
			inm.setUnique(true);
			inm.setCollectionName("cache");
			List<Integer> columnIdList = new ArrayList<Integer>();
			columnIdList.add(0);
			inm.setColumnIdList(columnIdList);
			String storageFile = cacheIndexStorage != null ? cacheIndexStorage : StorageMetadata.getInstance().getDefaultFileName();
//			String storageFile = "cacheIndex.data";
			SchemaMetadata.getInstance().createNewIndex(inm, storageFile);

		}

//        writer.shutdown();
//        primaryCacheHandler.shutdown();
//        secondaryCacheHandler.shutdown();
//        StorageMetadata.getInstance().shutdown();
//        ScatterGatherQueryExecutor.shutdown();
        
//        ClusterManagerFactory.getInstance().getClusterManager().shutdown();
//        WonderDBConnectionPool.getInstance().shutdown();
    }
	
	public void shutdown() {
        ScatterGatherQueryExecutor.shutdown();
        WonderDBList.shutdown();
        Logger.getLogger(getClass()).info("Shutdown");
		writer.shutdown();
        primaryCacheHandler.shutdown();
        secondaryCacheHandler.shutdown();
        StorageMetadata.getInstance().shutdown();
        Logger.getLogger(getClass()).info("Shutdown");
		FilePointerFactory.getInstance().shutdown();
	}	
}
