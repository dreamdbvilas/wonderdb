package org.wonderdb.block.record.manager;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.block.BlockManager;
import org.wonderdb.block.ListBlock;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.server.WonderDBCacheService;
import org.wonderdb.types.ByteArrayType;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.RecordId;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.TableRecord;

public class CacheTest {
	static Random random = new Random(System.currentTimeMillis());
	static ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();

	public static void main(String[] args) throws Exception {
		WonderDBCacheService.getInstance().init(args[0]);
		String shouldWrite = args[1];
		int noOfThreads = Integer.parseInt(args[2]);
		String type = args[3];
		int size = Integer.parseInt(args[4]);
		byte[] bytes = new byte[500];
		
		long stat = System.currentTimeMillis();
		if ("load".equals(type)) {
			for (int i = 0; i < size; i++) {
				CacheManager.getInstance().set((""+i).getBytes(), bytes);
				
			}
		}
		
		
		if ("test".equals(type)) {
			if ("true".equals(shouldWrite)) {
				Thread[] array = new Thread[noOfThreads];
				for (int i = 0; i < noOfThreads; i++) {
					TestRunnable1 r1 = new TestRunnable1(size);
					Thread t1 = new Thread(r1);
					array[i] = t1;
					t1.start();
				}
				
				for (int i = 0; i < noOfThreads; i++) {
					array[i].join();
				}
			}

		}

		if ("exhaustiveTest".equals(type)) {
			main2(args);
		}
		
		long end = System.currentTimeMillis()-stat;
		System.out.println("Total time: " + end);
		WonderDBCacheService.getInstance().shutdown();
	}
	
	public static void main2(String[] args) throws Exception {
//		WonderDBCacheService.getInstance().init(args[0]);
		String shouldWrite = args[1];
		int noOfThreads = Integer.parseInt(args[2]);
		
		byte[] b = CacheManager.getInstance().get(new String("4258").getBytes());
		long currentTime = System.currentTimeMillis();
		{
			Set<Object> pinnedBlocks = new HashSet<Object>();
			WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata("cache").getRecordList(new Shard(""));
			TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata("cache");
			ResultIterator iter = dbList.iterator(meta, pinnedBlocks);
			
			while (iter.hasNext()) {
				TableRecord record = (TableRecord) iter.next();
				DBType dt = record.getColumnMap().get(1);
				ByteArrayType bat = null;
				if (dt instanceof ExtendedColumn) {
					ColumnSerializer.getInstance().readFull(dt, new ColumnSerializerMetadata(SerializerManager.BYTE_ARRAY_TYPE));
					bat = (ByteArrayType) ((ExtendedColumn) dt).getValue(null);
				} else if (record instanceof ExtendedTableRecord) {
					RecordSerializer.getInstance().readFull(record, meta);
					bat = (ByteArrayType) record.getValue(1);
				} else {
					bat = (ByteArrayType) dt;
				}
				byte[] value = bat.get();
				ByteArrayType bat1 = (ByteArrayType) record.getValue(0);
				int key = Integer.parseInt(new String(bat1.get()));
				if (key == 4258) {
					int x = 0;
					x = 20;
				}
				map.put(key, value.length);
			}
			iter.unlock(true);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			
			
			IndexNameMeta inm = SchemaMetadata.getInstance().getIndex("cacheIndex");
			BTree tree = inm.getIndexTree(new Shard(""));
			iter = tree.getHead(false, pinnedBlocks);
			IndexKeyType prev = null;
			int x = 0;
			while (iter.hasNext()) {
				IndexRecord record = (IndexRecord) iter.next();
				IndexKeyType ikt = (IndexKeyType) record.getColumn();
				if (prev != null) {
					if (ikt.compareTo(prev) <= 0) {
						System.out.println("issue with key: " + new String(((ByteArrayType) ikt.getValue().get(0)).get()));
					}
				}
	//			System.out.println(x);
				x++;
				int key = Integer.parseInt(new String(((ByteArrayType) ikt.getValue().get(0)).get()));
				RecordId recordId = ikt.getRecordId();
				Set<Object> set = new HashSet<Object>();
				ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, set);
				CacheEntryPinner.getInstance().unpin(set, set);
				TableRecord tr = (TableRecord) block.getData().get(recordId.getPosn());
				DBType dt = tr.getColumnMap().get(1);
				ByteArrayType bat = null;
				if (dt instanceof ExtendedColumn) {
					ColumnSerializer.getInstance().readFull(dt, new ColumnSerializerMetadata(SerializerManager.BYTE_ARRAY_TYPE));
					bat = (ByteArrayType) ((ExtendedColumn) dt).getValue(null);
				} else if (tr instanceof ExtendedTableRecord) {
					RecordSerializer.getInstance().readFull(tr, meta);
					bat = (ByteArrayType) tr.getValue(1);
				} else {
					bat = (ByteArrayType) dt;
				}
				byte[] b1 = ((ByteArrayType) tr.getValue(0)).get();
				int key1 = Integer.parseInt(new String(b1));
				if (key != key1) {
					System.out.println("mismatch : index key is: " + key + "table key is: " + key1);
				}
				int size = bat.get().length;
				int s1 = map.get(key);
				if (size != s1) {
					System.out.println("issue with key:" + key);
				}
				prev = ikt;
			}
			iter.unlock(true);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			
			System.out.println("total: " + x);
		}
		
		long start = System.currentTimeMillis();
		if ("true".equals(shouldWrite)) {
			Thread[] array = new Thread[noOfThreads];
			for (int i = 0; i < noOfThreads; i++) {
				TestRunnable r1 = new TestRunnable();
				Thread t1 = new Thread(r1);
				array[i] = t1;
				t1.start();
			}
			
			for (int i = 0; i < noOfThreads; i++) {
				array[i].join();
			}
		}
		long end = System.currentTimeMillis()-start;
		System.out.println("total time to set = " + end);
		{
			Set<Object> pinnedBlocks = new HashSet<Object>();
			WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata("cache").getRecordList(new Shard(""));
			TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata("cache");
			ResultIterator iter = dbList.iterator(meta, pinnedBlocks);
			
			while (iter.hasNext()) {
				TableRecord record = (TableRecord) iter.next();
				DBType dt = record.getColumnMap().get(1);
				ByteArrayType bat = null;
				if (dt instanceof ExtendedColumn) {
					ColumnSerializer.getInstance().readFull(dt, new ColumnSerializerMetadata(SerializerManager.BYTE_ARRAY_TYPE));
					bat = (ByteArrayType) ((ExtendedColumn) dt).getValue(null);
				} else if (record instanceof ExtendedTableRecord) {
					RecordSerializer.getInstance().readFull(record, meta);
					bat = (ByteArrayType) record.getValue(1);
				} else {
					bat = (ByteArrayType) dt;
				}
				byte[] value = bat.get();
				ByteArrayType bat1 = (ByteArrayType) record.getValue(0);
				int key = Integer.parseInt(new String(bat1.get()));
				map.put(key, value.length);
			}
			iter.unlock(true);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			
			IndexNameMeta inm = SchemaMetadata.getInstance().getIndex("cacheIndex");
			BTree tree = inm.getIndexTree(new Shard(""));
			iter = tree.getHead(false, pinnedBlocks);
			IndexKeyType prev = null;
			int x = 0;
			while (iter.hasNext()) {
				IndexRecord record = (IndexRecord) iter.next();
				IndexKeyType ikt = (IndexKeyType) record.getColumn();
				if (prev != null) {
					if (ikt.compareTo(prev) <= 0) {
						System.out.println("issue with key: " + new String(((ByteArrayType) ikt.getValue().get(0)).get()));
					}
				}
				int key = Integer.parseInt(new String(((ByteArrayType) ikt.getValue().get(0)).get()));
				RecordId recordId = ikt.getRecordId();
				Set<Object> set = new HashSet<Object>();
				ListBlock block = (ListBlock) BlockManager.getInstance().getBlock(recordId.getPtr(), meta, set);
				CacheEntryPinner.getInstance().unpin(set, set);
				TableRecord tr = (TableRecord) block.getData().get(recordId.getPosn());
				DBType dt = tr.getColumnMap().get(1);
				ByteArrayType bat = null;
				if (dt instanceof ExtendedColumn) {
					ColumnSerializer.getInstance().readFull(dt, new ColumnSerializerMetadata(SerializerManager.BYTE_ARRAY_TYPE));
					bat = (ByteArrayType) ((ExtendedColumn) dt).getValue(null);
				} else if (tr instanceof ExtendedTableRecord) {
					RecordSerializer.getInstance().readFull(tr, meta);
					bat = (ByteArrayType) tr.getValue(1);
				} else {
					bat = (ByteArrayType) dt;
				}
				
				int size = bat.get().length;
				int s1 = map.get(key);
				if (size != s1) {
					System.out.println("problem with the key: " + key + "got length: " + size + "when expecting: " + s1);
				}
				prev = ikt;
			}
			iter.unlock(true);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);

			System.out.println("total: " + x);
		}
		long finish = System.currentTimeMillis();
		long total = finish - currentTime;
		System.out.println("time taken: " + total);
//		WonderDBCacheService.getInstance().shutdown();

	}	
		
	public static class TestRunnable implements Runnable {

		@Override
		public void run() {
//			byte[] b = new byte[1];
//			b[0]=1;
//			CacheManager.getInstance().set(b, b);
//			if (true) return;
			for (int i = 0; i < 10000; i++) {
				int key = Math.abs(random.nextInt()) % 1000000;
				int size = Math.abs(random.nextInt()) % 1000;
//				int size = 1000;
				byte[] bytes = new byte[size+1];
				byte[] k = new String(""+key).getBytes();
				Integer x = new Integer(key);
				synchronized (x) {
					int rec = CacheManager.getInstance().set(k, bytes);
					if (rec == 1) {
						map.put(key, bytes.length);
					}
				}
			}
		}		
	}

	public static class TestRunnable1 implements Runnable {
		int size = 0;
		public TestRunnable1(int size) {
			this.size = size;
		}
		
		public void run() {
			byte[] bytes = new byte[500];
			for (int i = 0; i < 100000; i++) {
				int r = Math.abs(random.nextInt()) % size;
				CacheManager.getInstance().get((""+r).getBytes());
//				CacheManager.getInstance().set((""+r).getBytes(), bytes);
				r = Math.abs(random.nextInt()) % size;
				byte[] b = CacheManager.getInstance().get((""+r).getBytes());
//				if (b == null || (b.length != 500 && b.length != 100)) {
//					System.out.println("error" + b.length);
//				}
			}
		}
	}
}
