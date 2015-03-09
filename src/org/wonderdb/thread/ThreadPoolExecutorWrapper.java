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
package org.wonderdb.thread;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExecutorWrapper {
	ThreadPoolExecutor executor = null;
	
	public ThreadPoolExecutorWrapper(String name) {
		WonderDBThreadFactory t = new WonderDBThreadFactory(name);
		executor = new ThreadPoolExecutor (10, 50, 5, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(20), t);
		
	}
	
	public ThreadPoolExecutorWrapper(int coreSize, int maxSize, int keepAliveInMin, int queueSize, String name) {
		WonderDBThreadFactory t = new WonderDBThreadFactory(name);		
		executor = new ThreadPoolExecutor(coreSize, maxSize, keepAliveInMin, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(queueSize), t);
	}
	
	public Object synchronousExecute(Callable<? extends Object> task) {
		Object retVal = null;
		try {
			retVal = executor.submit(task).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public Future<? extends Object> asynchrounousExecute(Callable<? extends Object> task) {
		return executor.submit(task);
	}
	
	public Future<?> asynchrounousExecute(Runnable task) {
		return executor.submit(task);
	}
	
	public void asynchronousFutureTask(FutureTask<? extends Object> task) {
		executor.execute(task);
	}
	
	public boolean isShutdown() {
		return executor.isShutdown();
	}
	
	public void shutdown() {
		if (isShutdown()) {
			return;
		}
		executor.shutdown();
		while (true) {
			try {
				executor.awaitTermination(2, TimeUnit.HOURS);
				break;				
			} catch (InterruptedException e) {
			}
		}
	}
}
