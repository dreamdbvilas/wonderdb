package org.wonderdb.server;
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

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
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
import org.wonderdb.cache.impl.CacheBean;
import org.wonderdb.cache.impl.CacheHandler;
import org.wonderdb.cache.impl.CacheLock;
import org.wonderdb.cache.impl.CacheState;
import org.wonderdb.cache.impl.CacheWriter;
import org.wonderdb.cache.impl.MemoryCacheMap;
import org.wonderdb.query.sql.WonderDBDriver;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.record.Record;


public class WonderDBServer {
	public static final int DEFAULT_BLOCK_SIZE = 2048;
	static final ChannelGroup allServerChannels = new DefaultChannelGroup("wonderdb-server");
	static final ChannelGroup allShardChannels = new DefaultChannelGroup("wonderdb-shard");
	static boolean shutdown = false;
	static Object lock = new Object();
	
	static CacheBean primaryCacheBean = new CacheBean();
	static CacheState primaryCacheState = new CacheState();
	static MemoryCacheMap<BlockPtr, List<Record>> primaryCacheMap = new MemoryCacheMap<BlockPtr, List<Record>>(1000, 5, false);
	static CacheLock cacheLock = new CacheLock();
	static CacheBean secondaryCacheBean = new CacheBean();
	static CacheState secondaryCacheState = new CacheState();
	static MemoryCacheMap<BlockPtr, ChannelBuffer> secondaryCacheMap = new MemoryCacheMap<BlockPtr, ChannelBuffer>(1500, 5, true);
//	static SecondaryCacheResourceProvider resourceProvider = null;
	static int colId = -1;
	static CacheHandler<BlockPtr, List<Record>> primaryCacheHandler = null;
	static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = null;
	public static CacheWriter<BlockPtr, ChannelBuffer> writer = null;
	
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
    		Logger.getRootLogger().fatal("Please provide init file at strartup");
    		return;
    	}
    	


    	WonderDBCacheService.getInstance().init(args[0]);
    	
		ExecutorService serverBoss = new ThreadPoolExecutor(WonderDBPropertyManager.getInstance().getNettyBossThreadPoolCoreSize(), 
				WonderDBPropertyManager.getInstance().getNettyBossThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(WonderDBPropertyManager.getInstance().getNettyBossThreadPoolQueueSize())); // 10, 50, 5, 20
		
		ExecutorService serverWorker = new ThreadPoolExecutor(WonderDBPropertyManager.getInstance().getNettyWorkerThreadPoolCoreSize(), 
				WonderDBPropertyManager.getInstance().getNettyWorkerThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(WonderDBPropertyManager.getInstance().getNettyWorkerThreadPoolQueueSize())); // 10, 50, 5, 20

		ChannelFactory serverFactory =
            new NioServerSocketChannelFactory(
                    Executors.unconfigurableExecutorService(serverBoss),
                    Executors.unconfigurableExecutorService(serverWorker));

        ServerBootstrap serverBootstrap = new ServerBootstrap(serverFactory);

        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new BufferDecoder(),
                						 new WonderDBShardServerHandler(WonderDBShardServerHandler.SERVER_HANDLER));
            }
        });

        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setOption("child.keepAlive", true);

        Channel serverChannel = serverBootstrap.bind(new InetSocketAddress(WonderDBPropertyManager.getInstance().getShardPort()));
        allServerChannels.add(serverChannel);

		ExecutorService shardBoss = new ThreadPoolExecutor(WonderDBPropertyManager.getInstance().getNettyBossThreadPoolCoreSize(), 
				WonderDBPropertyManager.getInstance().getNettyBossThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(WonderDBPropertyManager.getInstance().getNettyBossThreadPoolQueueSize())); // 10, 50, 5, 20
		
		ExecutorService shardWorker = new ThreadPoolExecutor(WonderDBPropertyManager.getInstance().getNettyWorkerThreadPoolCoreSize(), 
				WonderDBPropertyManager.getInstance().getNettyWorkerThreadPoolMaxSize(), 5, TimeUnit.MINUTES, 
				new ArrayBlockingQueue<Runnable>(WonderDBPropertyManager.getInstance().getNettyWorkerThreadPoolQueueSize())); // 10, 50, 5, 20

		ChannelFactory shardFactory =
            new NioServerSocketChannelFactory(
                    Executors.unconfigurableExecutorService(shardBoss),
                    Executors.unconfigurableExecutorService(shardWorker));

        ServerBootstrap shardBootstrap = new ServerBootstrap(shardFactory);

        shardBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new BufferDecoder(),
                						 new WonderDBShardServerHandler(WonderDBShardServerHandler.SHARD_HANDLER));
            }
        });

        shardBootstrap.setOption("child.tcpNoDelay", true);
        shardBootstrap.setOption("child.keepAlive", true);

        Channel shardChannel = shardBootstrap.bind(new InetSocketAddress(WonderDBPropertyManager.getInstance().getServerPort()));
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
        DriverManager.registerDriver(new WonderDBDriver());
        synchronized(lock) {
        	while (shutdown == false) {
        		try {
        			lock.wait();
        		} catch (Exception e) {
        		}
        	}
        }
        
        ChannelGroupFuture future = allServerChannels.close();
    	future = allServerChannels.close();
        future.awaitUninterruptibly();
    	future = allShardChannels.close();
        future.awaitUninterruptibly();
        shardFactory.releaseExternalResources();
        serverFactory.releaseExternalResources();

        WonderDBCacheService.getInstance().shutdown();
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