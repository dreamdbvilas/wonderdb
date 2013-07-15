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

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class WonderDBPropertyManager {
	private static final String SERVER_PORT = "server.port";
	private static final String REPLICASET_PORT = "replicaset.port";
	private static final String SHARD_PORT = "shard.port";
	private static final String SYSTEM_FILE = "systemFile";
	private static final String PRIMARY_CACHE_HIGH_WATERARK = "primaryCache.highWatermark";
	private static final String PRIMARY_CACHE_LOW_WATERARK = "primaryCache.lowWatermark";
	private static final String PRIMARY_CACHE_MAX_SIZE = "primaryCache.maxSize";
	private static final String SECONDARY_CACHE_HIGH_WATERARK = "secondaryCache.highWatermark";
	private static final String SECONDARY_CACHE_LOW_WATERARK = "secondaryCache.lowWatermark";
	private static final String SECONDARY_CACHE_MAX_SIZE = "secondaryCache.maxSize";
	private static final String CACHE_WRITER_SYNC_TIME = "cacheWriter.syncTime";
	private static final String WRITER_THREAD_POOL_CORE_SIZE = "writerThreadPool.coreSize";
	private static final String WRITER_THREAD_POOL_MAX_SIZE = "writerThreadPool.maxSize";
	private static final String WRITER_THREAD_POOL_QUEUE_SIZE = "writerThreadPool.queueSize";
	
	private static final String NETTY_BOSS_THREAD_POOL_CORE_SIZE = "nettyBossThreadPool.coreSize";
	private static final String NETTY_BOSS_THREAD_POOL_MAX_SIZE = "nettyBossThreadPool.maxSize";
	private static final String NETTY_BOSS_THREAD_POOL_QUEUE_SIZE = "nettyBossThreadPool.queueSize";
	private static final String NETTY_WORKER_THREAD_POOL_CORE_SIZE = "nettyWorkerThreadPool.coreSize";
	private static final String NETTY_WORKER_THREAD_POOL_MAX_SIZE = "nettyWorkerThreadPool.maxSize";
	private static final String NETTY_WORKER_THREAD_POOL_QUEUE_SIZE = "nettyWorkerThreadPool.queueSize";
	
	private static final String LOG_FILE_PATH = "logFilePath";
	private static final String LOOKAHEAD_LOGGING_ENABLED = "lookaheadLoggingEnabled";
	private static final String ARCHIVE_LOG_FILE_PATH = "archiveLogFilePath";

	private static final String ZK_CONNECT_STR = "zk.connect.string";
	private static final String ZK_CONNECT_RETRY_COUNT = "zk.connect.retry.count";
	private static final String ZK_CONNECT_RETRY_SLEEP_TIME = "zk.connect.retry.sleepTime";

	private static final String ZK_CONNECTION_TIMEOUT = "zk.connection.timeout";
	private static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";
	
	private static final String KAFKA_BROKER_LIST = "kafka.broker.list";
	private static final String KAFKA_REPLICATION_ENABLED = "kafka.replication.enabled";

	private static WonderDBPropertyManager instance = new WonderDBPropertyManager();
	private boolean initialized = false;
	private Properties properties = new Properties();
	
	private WonderDBPropertyManager() {
	}

	public static WonderDBPropertyManager getInstance() {
		return instance;
	}
	
	public synchronized void init(String propertyFile) throws IOException {
		if (initialized) {
			return;
		}
		FileReader reader = new FileReader(propertyFile);
		properties.load(reader);
		initialized = true;
	}
	
	public int getPrimaryCacheHighWatermark() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(PRIMARY_CACHE_HIGH_WATERARK);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + PRIMARY_CACHE_HIGH_WATERARK + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getPrimaryCacheLowWatermark() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(PRIMARY_CACHE_LOW_WATERARK);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + PRIMARY_CACHE_LOW_WATERARK + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getPrimaryCacheMaxSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(PRIMARY_CACHE_MAX_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + PRIMARY_CACHE_MAX_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getSecondaryCacheHighWatermark() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(SECONDARY_CACHE_HIGH_WATERARK);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + SECONDARY_CACHE_HIGH_WATERARK + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getSecondaryCacheLowWatermark() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(SECONDARY_CACHE_LOW_WATERARK);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + SECONDARY_CACHE_LOW_WATERARK + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getSecondaryCacheMaxSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(SECONDARY_CACHE_MAX_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + SECONDARY_CACHE_MAX_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getCacheWriterSyncTime() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(CACHE_WRITER_SYNC_TIME);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + CACHE_WRITER_SYNC_TIME + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getWriterThreadPoolCoreSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(WRITER_THREAD_POOL_CORE_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + WRITER_THREAD_POOL_CORE_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getWriterThreadPoolMaxSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(WRITER_THREAD_POOL_MAX_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + WRITER_THREAD_POOL_MAX_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getWriterThreadPoolQueueSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(WRITER_THREAD_POOL_QUEUE_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + WRITER_THREAD_POOL_QUEUE_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getNettyBossThreadPoolCoreSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(NETTY_BOSS_THREAD_POOL_CORE_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + NETTY_BOSS_THREAD_POOL_CORE_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getNettyBossThreadPoolMaxSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(NETTY_BOSS_THREAD_POOL_MAX_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + NETTY_BOSS_THREAD_POOL_MAX_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getNettyBossThreadPoolQueueSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(NETTY_BOSS_THREAD_POOL_QUEUE_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + NETTY_BOSS_THREAD_POOL_QUEUE_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getNettyWorkerThreadPoolCoreSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(NETTY_WORKER_THREAD_POOL_CORE_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + NETTY_WORKER_THREAD_POOL_CORE_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getNettyWorkerThreadPoolMaxSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(NETTY_WORKER_THREAD_POOL_MAX_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + NETTY_WORKER_THREAD_POOL_MAX_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getNettyWorkerThreadPoolQueueSize() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(NETTY_WORKER_THREAD_POOL_QUEUE_SIZE);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + NETTY_WORKER_THREAD_POOL_MAX_SIZE + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getServerPort() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(SERVER_PORT);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + SERVER_PORT + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getShardPort() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(SHARD_PORT);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + SHARD_PORT + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getReplicaSetPort() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(REPLICASET_PORT);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + REPLICASET_PORT + " is not set");
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public String getLogFilePath() {
		
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(LOG_FILE_PATH);
		if (val == null) {
			throw new RuntimeException("Server Property " + LOG_FILE_PATH + " is not set");
		}
		return val;
	}
	
	public String getArchiveLogFilePath() {
		
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(ARCHIVE_LOG_FILE_PATH);
		if (val == null || val.length() == 0) {
			throw new RuntimeException("Server Property " + ARCHIVE_LOG_FILE_PATH + " is not set");
		}
		return val;
	}
	
	public String getZkConnString() {
		
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(ZK_CONNECT_STR);
		return val;
	}

	public int getZkConnectRetryCount() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(ZK_CONNECT_RETRY_COUNT);
		if (val == null || val.length() == 0) {
			return 3;
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getZkConnectRetrySleepTime() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(ZK_CONNECT_RETRY_SLEEP_TIME);
		if (val == null || val.length() == 0) {
			return 1000;
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getZkConnectionTimeout() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(ZK_CONNECTION_TIMEOUT);
		if (val == null || val.length() == 0) {
			return 30000;
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public int getZkSessionTimeout() {
		if (!initialized) {
			throw new RuntimeException("Server not initialized properly");
		}
		
		String val = properties.getProperty(ZK_SESSION_TIMEOUT);
		if (val == null || val.length() == 0) {
			return 30000;
		}
		
		int v = Integer.parseInt(val);
		return v;
	}
	
	public String getSystemFile() {
		String val = properties.getProperty(SYSTEM_FILE);
		if (val == null || val.length() == 0) {
			return "system";
		}
		return val;
	}
	
	public String getKafkaBrokerList() {
		return properties.getProperty(KAFKA_BROKER_LIST);
	}
	
	public boolean isReplicationEnabled() {
		String value = properties.getProperty(KAFKA_REPLICATION_ENABLED);
		if ("true".equals(value)) {
			return true;
		}
		return false;
	}
	
	public boolean isLookAheadLoggingEnabled() {
		String value = properties.getProperty(LOOKAHEAD_LOGGING_ENABLED);
		if ("true".equals(value)) {
			return true;
		}
		return false;
	}	
}
