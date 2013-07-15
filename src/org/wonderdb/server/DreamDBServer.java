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
package org.wonderdb.server;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.wonderdb.block.Block;
import org.wonderdb.block.CacheableList;
import org.wonderdb.cache.BaseCacheHandler;
import org.wonderdb.cache.CacheBean;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.CacheLock;
import org.wonderdb.cache.CacheMap;
import org.wonderdb.cache.CacheState;
import org.wonderdb.cache.CacheUsage;
import org.wonderdb.cache.CacheWriter;
import org.wonderdb.cache.InflightFileReader;
import org.wonderdb.cache.Pinner;
import org.wonderdb.cache.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.PrimaryCacheResourceProvider;
import org.wonderdb.cache.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.SecondaryCacheResourceProvider;
import org.wonderdb.cache.SecondaryCacheResourceProviderFactory;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.kafka.KafkaConsumerManager;
import org.wonderdb.cluster.kafka.KafkaProducerManager;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.query.sql.DreamDBConnectionPool;
import org.wonderdb.query.sql.DreamDBDriver;
import org.wonderdb.schema.CollectionTailMgr;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.block.SerializedBlock;


public class DreamDBServer {
	public static final int DEFAULT_BLOCK_SIZE = 2048;
	static final ChannelGroup allServerChannels = new DefaultChannelGroup("wonderdb-server");
	static final ChannelGroup allShardChannels = new DefaultChannelGroup("wonderdb-shard");
	static boolean shutdown = false;
	static Object lock = new Object();
	
	static CacheBean primaryCacheBean = new CacheBean();
	static CacheState primaryCacheState = new CacheState();
	static CacheMap<CacheableList> primaryCacheMap = new CacheMap<CacheableList>(1000, 5, false);
	static CacheLock cacheLock = new CacheLock();
	static CacheBean secondaryCacheBean = new CacheBean();
	static CacheState secondaryCacheState = new CacheState();
	static CacheMap<ChannelBuffer> secondaryCacheMap = new CacheMap<ChannelBuffer>(1500, 5, true);
//	static SecondaryCacheResourceProvider resourceProvider = null;
	static int colId = -1;
	static CacheHandler<CacheableList> primaryCacheHandler = null;
	static CacheHandler<ChannelBuffer> secondaryCacheHandler = null;
	public static CacheWriter writer = null;
	
	static {
		File file = new File("./log4j.properties");
		if (file.exists()) {
			PropertyConfigurator.configure("./log4j.properties");
		} else {
			String val = System.getProperty("log4j.configuration");
			file = null;
			if (val != null && val.length() > 0) {
				file = new File(val);
			}
			if (file != null && file.exists()) {
				PropertyConfigurator.configure(val);
			} else {
				BasicConfigurator.configure();
			}
		}
	}
    public static void main(String[] args) throws Exception {
    	if (args == null || args.length != 1) {
    		System.out.println("Please provide init file at strartup");
    		return;
    	}
    	
    	DreamDBPropertyManager.getInstance().init(args[0]);
    	
		primaryCacheBean.setCleanupHighWaterMark(DreamDBPropertyManager.getInstance().getPrimaryCacheHighWatermark()); // 1000
		primaryCacheBean.setCleanupLowWaterMark(DreamDBPropertyManager.getInstance().getPrimaryCacheLowWatermark()); // 999
		primaryCacheBean.setMaxSize(DreamDBPropertyManager.getInstance().getPrimaryCacheMaxSize()); // 1000
		PrimaryCacheResourceProvider primaryProvider = new PrimaryCacheResourceProvider(primaryCacheBean, primaryCacheState, cacheLock);
		PrimaryCacheResourceProviderFactory.getInstance().setResourceProvider(primaryProvider);
		primaryCacheHandler = new BaseCacheHandler<Block, CacheableList>(primaryCacheMap, primaryCacheBean, primaryCacheState, 
				cacheLock, primaryProvider, null, null);
		PrimaryCacheHandlerFactory.getInstance().setCacheHandler(primaryCacheHandler);
		
		secondaryCacheBean.setCleanupHighWaterMark(DreamDBPropertyManager.getInstance().getSecondaryCacheHighWatermark()); // 1475
		secondaryCacheBean.setCleanupLowWaterMark(DreamDBPropertyManager.getInstance().getSecondaryCacheLowWatermark()); // 1450
		secondaryCacheBean.setMaxSize(DreamDBPropertyManager.getInstance().getSecondaryCacheMaxSize()); // 1500
//		secondaryCacheBean.setStartSyncSize(10);
		CacheLock secondaryCacheLock = new CacheLock();
		SecondaryCacheResourceProvider secondaryProvider = new SecondaryCacheResourceProvider(secondaryCacheBean, secondaryCacheState, 
				secondaryCacheLock, DreamDBPropertyManager.getInstance().getSecondaryCacheMaxSize());
		SecondaryCacheResourceProviderFactory.getInstance().setResourceProvider(secondaryProvider);
		secondaryCacheHandler = new BaseCacheHandler<SerializedBlock, ChannelBuffer>(secondaryCacheMap, 
				secondaryCacheBean, secondaryCacheState, secondaryCacheLock, secondaryProvider, InflightFileReader.getInstance(), null);
		SecondaryCacheHandlerFactory.getInstance().setCacheHandler(secondaryCacheHandler);
		writer = new CacheWriter(secondaryCacheMap, DreamDBPropertyManager.getInstance().getCacheWriterSyncTime()); // 30000

		SchemaMetadata.getInstance().initialize();

		MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = new ObjectName("PrimaryCacheState:type=CacheUsage");
		beanServer.registerMBean(new CacheUsage(primaryCacheState), name);
		name = new ObjectName("ScondaryCacheState:type=CacheUsage");
		beanServer.registerMBean(new CacheUsage(secondaryCacheState), name);
		name = new ObjectName("CacahePinner:type=Pinner");
		beanServer.registerMBean(new Pinner(), name);

		ExecutorService serverBoss = new ThreadPoolExecutor(DreamDBPropertyManager.getInstance().getNettyBossThreadPoolCoreSize(), 
				DreamDBPropertyManager.getInstance().getNettyBossThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(DreamDBPropertyManager.getInstance().getNettyBossThreadPoolQueueSize())); // 10, 50, 5, 20
		
		ExecutorService serverWorker = new ThreadPoolExecutor(DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolCoreSize(), 
				DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolQueueSize())); // 10, 50, 5, 20

		ChannelFactory serverFactory =
            new NioServerSocketChannelFactory(
                    Executors.unconfigurableExecutorService(serverBoss),
                    Executors.unconfigurableExecutorService(serverWorker));

        ServerBootstrap serverBootstrap = new ServerBootstrap(serverFactory);

        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new BufferDecoder(),
                						 new DreamDBShardServerHandler(DreamDBShardServerHandler.SERVER_HANDLER));
            }
        });

        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setOption("child.keepAlive", true);

        Channel serverChannel = serverBootstrap.bind(new InetSocketAddress(DreamDBPropertyManager.getInstance().getShardPort()));
        allServerChannels.add(serverChannel);

		ExecutorService shardBoss = new ThreadPoolExecutor(DreamDBPropertyManager.getInstance().getNettyBossThreadPoolCoreSize(), 
				DreamDBPropertyManager.getInstance().getNettyBossThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(DreamDBPropertyManager.getInstance().getNettyBossThreadPoolQueueSize())); // 10, 50, 5, 20
		
		ExecutorService shardWorker = new ThreadPoolExecutor(DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolCoreSize(), 
				DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolQueueSize())); // 10, 50, 5, 20

		ChannelFactory shardFactory =
            new NioServerSocketChannelFactory(
                    Executors.unconfigurableExecutorService(shardBoss),
                    Executors.unconfigurableExecutorService(shardWorker));

        ServerBootstrap shardBootstrap = new ServerBootstrap(shardFactory);

        shardBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new BufferDecoder(),
                						 new DreamDBShardServerHandler(DreamDBShardServerHandler.SHARD_HANDLER));
            }
        });

        shardBootstrap.setOption("child.tcpNoDelay", true);
        shardBootstrap.setOption("child.keepAlive", true);

        Channel shardChannel = shardBootstrap.bind(new InetSocketAddress(DreamDBPropertyManager.getInstance().getServerPort()));
        allShardChannels.add(shardChannel);

//		ExecutorService replicaSetBoss = new ThreadPoolExecutor(DreamDBPropertyManager.getInstance().getNettyBossThreadPoolCoreSize(), 
//				DreamDBPropertyManager.getInstance().getNettyBossThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
//				new ArrayBlockingQueue<Runnable>(DreamDBPropertyManager.getInstance().getNettyBossThreadPoolQueueSize())); // 10, 50, 5, 20
//		
//		ExecutorService replicaSetWorker = new ThreadPoolExecutor(DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolCoreSize(), 
//				DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
//				new ArrayBlockingQueue<Runnable>(DreamDBPropertyManager.getInstance().getNettyWorkerThreadPoolQueueSize())); // 10, 50, 5, 20
//
//		ChannelFactory replicaSetFactory =
//            new NioServerSocketChannelFactory(
//                    Executors.unconfigurableExecutorService(replicaSetBoss),
//                    Executors.unconfigurableExecutorService(replicaSetWorker));
//
//        ServerBootstrap replicaSetBootstrap = new ServerBootstrap(replicaSetFactory);
//
//        replicaSetBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
//            public ChannelPipeline getPipeline() {
//                return Channels.pipeline(new BufferDecoder(),
//                						 new DreamDBShardServerHandler(DreamDBShardServerHandler.REPLICASET_HANDLER));
//            }
//        });
//
//        replicaSetBootstrap.setOption("child.tcpNoDelay", true);
//        replicaSetBootstrap.setOption("child.keepAlive", true);

//        Channel replicaSetChannel = shardBootstrap.bind(new InetSocketAddress(DreamDBPropertyManager.getInstance().getReplicaSetPort()));
//        allChannels.add(replicaSetChannel);
        DriverManager.registerDriver(new DreamDBDriver());
        synchronized(lock) {
        	while (shutdown == false) {
        		try {
        			lock.wait();
        		} catch (Exception e) {
        		}
        	}
        }
        writer.shutdown();
//        ArchiveLogRemoteWriter.getInstance().shutdown();
//        replicaSetFactory.releaseExternalResources();
        primaryCacheHandler.shutdown();
        secondaryCacheHandler.shutdown();
        CollectionTailMgr.getInstance().shutdown();
        KafkaConsumerManager.getInstance().shutdown();
        ScatterGatherQueryExecutor.shutdown();
        ClusterManagerFactory.getInstance().getClusterManager().shutdown();
        KafkaProducerManager.getInstance().shutdown();
        DreamDBConnectionPool.getInstance().shutdown();
        
        ChannelGroupFuture future = allServerChannels.close();
    	future = allServerChannels.close();
        future.awaitUninterruptibly();
    	future = allShardChannels.close();
        future.awaitUninterruptibly();
        shardFactory.releaseExternalResources();
        serverFactory.releaseExternalResources();
    }
    
    public static class BufferDecoder extends FrameDecoder {
        @Override
        protected Object decode(
                ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
                
        	if (buffer.capacity() >= 4) {
        		buffer.resetReaderIndex();
        		int count = buffer.readInt();
        		buffer.resetReaderIndex();
        		if (buffer.capacity() < count+4) {
        			return null;
        		} else {
        			buffer.clear();
        			buffer.writerIndex(buffer.capacity());
        			buffer.readerIndex(4);
        			return buffer.readBytes(buffer.readableBytes());
        		}
        	}
        	return null;
        }    	
    }
}