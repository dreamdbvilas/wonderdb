package org.wonderdb.thread;

import java.util.concurrent.ThreadFactory;

public class WonderDBThreadFactory implements ThreadFactory {
	private String name = null;
	
	public WonderDBThreadFactory(String name) {
		this.name = name;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		return new Thread(r, name);
	}
}
