package org.wonderdb.query.executor;

import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.thread.ThreadPoolExecutorWrapper;

public class ScatterGatherThreadPool {
	ThreadPoolExecutorWrapper executor = new ThreadPoolExecutorWrapper(WonderDBPropertyManager.getInstance().getWriterThreadPoolCoreSize(),
			WonderDBPropertyManager.getInstance().getWriterThreadPoolMaxSize(), 5, 
			WonderDBPropertyManager.getInstance().getWriterThreadPoolQueueSize());

	private static ScatterGatherThreadPool instance = new ScatterGatherThreadPool();
	
	private ScatterGatherThreadPool() {
	}
	
	public static ScatterGatherThreadPool getInstance() {
		return instance;
	}
	
	public void shutdown() {
		executor.shutdown();
	}
	
	public ThreadPoolExecutorWrapper getThreadPool() {
		return executor;
	}
}
