package org.wonderdb.cache;

import org.wonderdb.cache.impl.CacheState;


public class CacheUsage implements CacheUsageMBean {
	CacheState cacheState = null;
	
	public CacheUsage(CacheState cacheState) {
		this.cacheState = cacheState;
	}

	@Override
	public int getState() {
		return (int) cacheState.getTotalCount();
	}
}
