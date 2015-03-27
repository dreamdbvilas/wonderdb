package org.wonderdb.server;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.CacheUsage;
import org.wonderdb.cache.Pinner;
import org.wonderdb.cache.impl.BaseCacheHandler;
import org.wonderdb.cache.impl.CacheBean;
import org.wonderdb.cache.impl.CacheEntryPinner;
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
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.UQLParser;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.query.parse.DBInsertQuery;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree.ResultSetValue;
import org.wonderdb.query.plan.DataContext;
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
						
			FileBlockEntry fbe = new FileBlockEntry();
			fbe.setBlockSize(2048);
			fbe.setFileName("cache.data");
			StorageMetadata.getInstance().add(fbe);

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
			
			SchemaMetadata.getInstance().createNewCollection("cache", null, columns, 10);
			
			IndexNameMeta inm = new IndexNameMeta();
			inm.setIndexName("cacheIndex");
			inm.setAscending(true);
			inm.setUnique(true);
			inm.setCollectionName("cache");
			List<Integer> columnIdList = new ArrayList<Integer>();
			columnIdList.add(0);
			inm.setColumnIdList(columnIdList);
//			String storageFile = StorageMetadata.getInstance().getDefaultFileName();
			String storageFile = "cache.data";
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
        primaryCacheHandler.shutdown();
        secondaryCacheHandler.shutdown();
        ScatterGatherQueryExecutor.shutdown();
        WonderDBList.shutdown();
//        Thread.currentThread().sleep(60000);
        System.out.println("Shutdown");
		writer.shutdown();
        StorageMetadata.getInstance().shutdown();
        System.out.println("Shutdown");
		FilePointerFactory.getInstance().shutdown();
	}
	
	public static void main(String[] args) throws Exception {
		WonderDBCacheService.getInstance().init(args[0]);
		Set<Object> pinnedBlocks = new HashSet<Object>();
//		WonderDBList list = SchemaMetadata.getInstance().createNewList("vilas", 5, new ColumnSerializerMetadata(SerializerManager.STRING));
//		WonderDBList list = SchemaMetadata.getInstance().getCollectionMetadata("vilas").getRecordList(new Shard(""));
//		StringType st = new StringType("athavale");
//		Column column = new Column(st);
//		ObjectListRecord record = new ObjectListRecord(column);
//		TypeMetadata meta = new ColumnSerializerMetadata(SerializerManager.STRING);
//		list.add(record, null, meta, pinnedBlocks);
//		ResultIterator iter = list.iterator(meta, pinnedBlocks);
//		try {
//			while (iter.hasNext()) {
//				ObjectListRecord reord = (ObjectListRecord) iter.next();
//			}
//		} finally {
//			iter.unlock(true);
//		}

//		List<IntType> list = new 
//		IndexNameMeta inm = new IndexNameMeta();
//		inm.setAscending(true);
//		inm.setCollectionName("");
//		inm.setColumnIdList(columnIdList);
		
//		SchemaMetadata.getInstance().createBTree("vilas", true, SerializerManager.STRING);
//		BTree tree = SchemaMetadata.getInstance().getIndex("vilas").getTree();
//		List<DBType> list = new ArrayList<>();
//		list.add(new StringType("vilas"));
//		IndexKeyType ikt = new IndexKeyType(list, null);
//		TransactionId txnId = org.wonderdb.txnlogger.LogManager.getInstance().startTxn();
//		tree.insert(ikt, pinnedBlocks, txnId);
//		ResultIterator iter = tree.iterator();
//		while (iter.hasNext()) {
//			Object o = iter.next();
//			int i = 0;
//		}
//		iter.unlock(true);

		String query = null;
		InputStream is = null;
		UQLParser parser = null;
		SimpleNode node = null;
		
//		query = "create default storage storage 'vilas' 4096";
//		is = new ByteArrayInputStream(query.getBytes());
//		parser = new UQLParser(is);
//		node = parser.Start();
//		DBCreateStorageQuery q = new DBCreateStorageQuery(query, node);
//		q.execute();

//		query = "create table vilas (a int, b string)";
//		is = new ByteArrayInputStream(query.getBytes());
//		parser = new UQLParser(is);
//		node = parser.Start();
//		DBCreateTableQuery q1 = new DBCreateTableQuery(query, node);
//		q1.execute();

//		query = "create unique index x2 on vilas(a)";
//		is = new ByteArrayInputStream(query.getBytes());
//		parser = new UQLParser(is);
//		node = parser.Start();
//		CreateIndexQuery q2 = new CreateIndexQuery(query, node);
//		q2.execute();

		query = "insert into vilas (a, b) values (1, 2);";
		is = new ByteArrayInputStream(query.getBytes());
		parser = new UQLParser(is);
		node = parser.Start();
		ChannelBuffer buffer = ChannelBuffers.buffer(10000);
		DBInsertQuery q3 = new DBInsertQuery(query, node, 0, new DataContext(), buffer);
		int i = q3.execute();
		
//		CollectionMetadata colMetadata = SchemaMetadata.getInstance().getCollectionMetadata("vilas");
//		WonderDBList dbList = colMetadata.getRecordList(new Shard(""));
//		TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata("vilas");
//		ResultIterator iter = dbList.iterator(meta, pinnedBlocks);
//		while (iter.hasNext()) {
//			TableRecord record = (TableRecord) iter.next();		
//			int i = 0;
//		}
//		iter.unlock(true);

		query = "select * from vilas where a <= 2";
		is = new ByteArrayInputStream(query.getBytes());
		parser = new UQLParser(is);
		node = parser.Start();
		buffer = ChannelBuffers.buffer(10000);
		DBSelectQueryJTree q4 = new DBSelectQueryJTree(query, node, node, 1, buffer);
		List<List<ResultSetValue>> l = q4.execute();
		System.out.println(l.size());
		
//		query = "update vilas set a = 2 where a = 1";
//		is = new ByteArrayInputStream(query.getBytes());
//		parser = new UQLParser(is);
//		node = parser.Start();
//		ChannelBuffer buffer = ChannelBuffers.buffer(10000);
//		DBUpdateQuery q4 = new DBUpdateQuery(query, node, new ArrayList<>(), 0, buffer);
//		q4.execute(new Shard(""));
		
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);

		writer.shutdown();
        primaryCacheHandler.shutdown();
        secondaryCacheHandler.shutdown();
        ScatterGatherQueryExecutor.shutdown();
//        Thread.currentThread().sleep(60000);
        System.out.println("Shutdown");
        StorageMetadata.getInstance().shutdown();
        System.out.println("Shutdown");
	}
}
