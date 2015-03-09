package org.wonderdb.freeblock;


import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.thread.ThreadPoolExecutorWrapper;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;


public class FreeBlockFactory {
	private static FreeBlockFactory instance = new FreeBlockFactory();
	ConcurrentMap<Byte, FreeBlockMgrNew> freeBlocksMgrMap = new ConcurrentHashMap<>();
	ThreadPoolExecutorWrapper threadPool = new ThreadPoolExecutorWrapper(5, 5, 5, 10, "freeBlock");
	static long val = 2048*100;
	private FreeBlockFactory() {
	}
	
	public static FreeBlockFactory getInstance() {
		return instance;
	}
	
	public void shutdown() {
		threadPool.shutdown();
	}
	
	public void createNewMgr(byte fileId, TransactionId txnId, int lowWatermarkMultiplier, int highWatermarkMultiplier) {
		FreeBlockMgrNew fbm = new FreeBlockMgrNew(fileId, txnId, lowWatermarkMultiplier, highWatermarkMultiplier);
		freeBlocksMgrMap.put(fileId, fbm);
	}

	public void createNewMgr(byte fileId, int lowWatermarkMultiplier, int highWatermarkMultiplier) {
		FreeBlockMgrNew fbm = new FreeBlockMgrNew(fileId, lowWatermarkMultiplier, highWatermarkMultiplier);
		freeBlocksMgrMap.put(fileId, fbm);
	}

	public void initialize(byte fileId, int lowWatermarkMultiplier, int highWatermarkMultiplier) {
		FreeBlockMgrNew fbm = new FreeBlockMgrNew(fileId, lowWatermarkMultiplier, highWatermarkMultiplier);		
		freeBlocksMgrMap.put(fileId, fbm);
	}

	public long getFreeBlockPosn(byte fileId) {
//		long retVal = val;
//		val = val + 2048;
//		return retVal;
		return freeBlocksMgrMap.get(fileId).getFreePosn();
	}
	
	public void returnBlock(BlockPtr ptr) {
//		if (ptr != null && ptr.getFileId() >= 0) {
//			freeBlocksMgrMap.get(ptr.getFileId()).returnBlock(ptr);
//		}
	}
	
	public void triggerWrite(FreeBlockMgrNew freeBlockMgr) {
		ReadFromBackEnd rfbe = new ReadFromBackEnd(freeBlockMgr, false);
		threadPool.asynchrounousExecute(rfbe);
	}
	
	public void triggerRead(FreeBlockMgrNew freeBlockMgr) {
		ReadFromBackEnd rfbe = new ReadFromBackEnd(freeBlockMgr, true);
		threadPool.asynchrounousExecute(rfbe);
	}
	
	private static class ReadFromBackEnd implements Callable<Boolean> {
		FreeBlockMgrNew freeBlockMgr = null;
		boolean isRead = false;
		
		private ReadFromBackEnd(FreeBlockMgrNew freeBlockMgr, boolean isRead) {
			this.freeBlockMgr = freeBlockMgr;
			this.isRead = isRead;
		}
		
		@Override
		public Boolean call() throws Exception {
			if (isRead) {
				freeBlockMgr.read();
			} else {
				freeBlockMgr.write();
			}
			return true;
		}
	}
}
