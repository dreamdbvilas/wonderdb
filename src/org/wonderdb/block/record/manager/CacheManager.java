package org.wonderdb.block.record.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.wonderdb.block.IndexCompareIndexQuery;
import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.cluster.Shard;
import org.wonderdb.core.collection.BTree;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.parser.jtree.ParseException;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParser;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.ColumnSerializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.record.RecordSerializer;
import org.wonderdb.server.WonderDBCacheService;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ByteArrayType;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.ExtendedTableRecord;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.TableRecord;

public class CacheManager {
	static Random random = new Random(System.currentTimeMillis());
	static ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();
	
	private static CacheManager instance = new CacheManager();
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

	private CacheManager() {
	}
	
	public static CacheManager getInstance() {
		return instance;
	}
	
	public void set(byte[] key, byte[] value) {
		Map<Integer, DBType> map = new HashMap<>();
		map.put(0, new ByteArrayType(key));
		map.put(1, new ByteArrayType(value));
		Shard shard = new Shard("");
		try {
			TableRecordManager.getInstance().addTableRecord("cache", map, shard, true);
		} catch (InvalidCollectionNameException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void remove(byte[] key) {
		TransactionId txnId = null;
		Set<Object> pinnedBlocks = new HashSet<>();
		try {
			List<DBType> list = new ArrayList<DBType>(1);
			list.add(new ByteArrayType(key));
			IndexKeyType ikt = new IndexKeyType(list, null);
			IndexNameMeta inm = SchemaMetadata.getInstance().getIndex("cacheIndex");
			BTree tree = inm.getIndexTree(new Shard(""));
			TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(inm);
			IndexCompareIndexQuery iciq = new IndexCompareIndexQuery(ikt, true, meta, pinnedBlocks);
			ResultIterator iter = tree.find(iciq, false, pinnedBlocks);
			IndexRecord ir = null;
			try {
				while (iter.hasNext()) {
					ir = (IndexRecord) iter.next();
					break;
				}
			} finally {
				iter.unlock(true);
			}
			
			IndexKeyType entry = null;
			if (ir != null) {
				DBType column = ir.getColumn();
				if (column instanceof ExtendedColumn) {
					entry = (IndexKeyType) ((ExtendedColumn) column).getValue(meta);
				} else {
					entry = (IndexKeyType) column;
				}
				txnId = LogManager.getInstance().startTxn();
				tree.remove(entry, pinnedBlocks, txnId);
				CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata("cache");
				WonderDBList dbList = colMeta.getRecordList(new Shard(""));
				meta = SchemaMetadata.getInstance().getTypeMetadata("cache");
				dbList.deleteRecord(entry.getRecordId(), txnId, meta, pinnedBlocks);
			}
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
			
		}
	}

	public byte[] get(byte[] key) {
		String query = "select value from cache where key = ?;";
		UQLParser parser = new UQLParser(query);
		SimpleNode selectNode = null;
		try {
			selectNode = parser.Start();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ByteArrayType bat = new ByteArrayType(key);
		SimpleNode n = SimpleNodeHelper.getInstance().getFirstNode(selectNode, org.wonderdb.parser.jtree.UQLParserTreeConstants.JJTQ);
		n.jjtSetValue(bat);
		DBSelectQueryJTree q = new DBSelectQueryJTree(query, selectNode, selectNode, 0, null);
		List<Map<CollectionAlias, TableRecord>> list = q.executeAndGetTableRecord(new Shard(""));
		if (list == null || list.size() == 0) {
			return null;
		}
		Iterator<TableRecord> iter = list.get(0).values().iterator();
		while (iter.hasNext()) {
			TableRecord tr = iter.next();
			bat = (ByteArrayType) tr.getValue(1);
			return bat.get();
		}
		return null;
	}
//	public byte[] get(byte[] key) {
//		ByteArrayType bat = new ByteArrayType(key);
//		Set<Object> pinnedBlocks = new HashSet<>();
//		IndexNameMeta inm = SchemaMetadata.getInstance().getIndex("_cacheIndex");
//		TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(inm);
//		Shard shard = new Shard("");
//		
//		List<DBType> list = new ArrayList<>();
//		list.add(bat);
//		IndexKeyType ikt = new IndexKeyType(list, null);
//		BTree tree = inm.getIndexTree(shard);
//		IndexQuery entry = new IndexCompareIndexQuery(ikt, true, meta, pinnedBlocks);
//		ResultIterator iter = tree.find(entry, false, pinnedBlocks);
//		while (iter.hasNext()) {
//			IndexRecord record = (IndexRecord) iter.next();
//			
//		}
//	}
	
	public static void main(String[] args) throws Exception {
		WonderDBCacheService.getInstance().init(args[0]);
		TestRunnable r1 = new TestRunnable();
		TestRunnable r2 = new TestRunnable();
		TestRunnable r3 = new TestRunnable();
		TestRunnable r4 = new TestRunnable();
		TestRunnable r5 = new TestRunnable();
		
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		Thread t3 = new Thread(r3);
		Thread t4 = new Thread(r4);
		Thread t5 = new Thread(r5);
		
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
//
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		
		
		Set<Object> pinnedBlocks = new HashSet<>();
		WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata("cache").getRecordList(new Shard(""));
		TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata("cache");
		ResultIterator iter = dbList.iterator(meta, pinnedBlocks);
		int i = 0;
		
		while (iter.hasNext()) {
			System.out.println("record read" + i++);
			if (i == 4429) {
				int x = 0;
				x = 20;
			}
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
		}
		iter.unlock(true);
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		System.out.println("total records : " + i);
		
		
		IndexNameMeta inm = SchemaMetadata.getInstance().getIndex("cacheIndex");
		BTree tree = inm.getIndexTree(new Shard(""));
		iter = tree.getHead(false, pinnedBlocks);
		IndexKeyType prev = null;
		int x = 0;
		while (iter.hasNext()) {
			IndexRecord record = (IndexRecord) iter.next();
			IndexKeyType ikt = (IndexKeyType) record.getColumn();
			System.out.println("Read: " + new String(((ByteArrayType) ikt.getValue().get(0)).get()));
			if (prev != null) {
				if (ikt.compareTo(prev) <= 0) {
					i = 0;
					i = 20;
				}
			}
//			System.out.println(x);
			x++;
			prev = ikt;
		}
		iter.unlock(true);
		System.out.println("total: " + x);
		WonderDBCacheService.getInstance().shutdown();

	}	
	
	public static void main1(String[] args) throws Exception {
		WonderDBCacheService.getInstance().init(args[0]);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000; i++) {
			sb.append("b");
		}
//		StringBuilder sb1 = new StringBuilder();
//		for (int i = 0; i < 2500; i++) {
//			sb1.append("a");
//		}
//		for (int i = 0; i < 7800; i++) {
//			System.out.println("reading: " + i);
//			byte[] bytes = CacheManager.getInstance().get((""+i).getBytes());
//		}
		IndexNameMeta inm = SchemaMetadata.getInstance().getIndex("cacheIndex");
		BTree tree = inm.getIndexTree(new Shard(""));
		Set<Object> pinnedBlocks = new HashSet<>();
//		ResultIterator iter = tree.getHead(false, pinnedBlocks);
//		IndexKeyType prev = null;
//		int x = 0;
//		while (iter.hasNext()) {
//			IndexRecord record = (IndexRecord) iter.next();
//			IndexKeyType ikt = (IndexKeyType) record.getColumn();
//			System.out.println("Read: " + new String(((ByteArrayType) ikt.getValue().get(0)).get()));
//			if (prev != null) {
//				if (ikt.compareTo(prev) <= 0) {
//					int i = 0;
//					i = 20;
//				}
//			}
//			if (x == 18795) {
//				int i = 0;
//				i = 20;
//			}
//			x++;
//			prev = ikt;
//		}
//		for (int i = 0; i < 1000; i++) {
//			System.out.println(i);
//			if (i == 18796) {
//				int x = 100;
//				x = 0;
//			}
//			CacheManager.getInstance().set((""+i).getBytes(), sb.toString().getBytes());
////			if (value == null) {
////				int x = 100;
////				x = 0;				
////			}
//		}
//		CacheManager.getInstance().set("vilas".getBytes(), "athavale".getBytes());
//		CacheManager.getInstance().remove(sb.toString().getBytes());
//		byte[] values = CacheManager.getInstance().get("vilas".getBytes());
//		values = CacheManager.getInstance().get(new String(""+451).getBytes());
//
		WonderDBList dbList = SchemaMetadata.getInstance().getCollectionMetadata("cache").getRecordList(new Shard(""));
		TypeMetadata meta = SchemaMetadata.getInstance().getTypeMetadata("cache");
		ResultIterator iter = dbList.iterator(meta, pinnedBlocks);
		int i = 0;
		while (iter.hasNext()) {
			System.out.println("record read" + i++);
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
			if (i == 105) {
				int c = 0;
				c = 10;
			}
			if (value == null) {
				System.out.println("deleted: " + i);
			} else {
				String s = new String(value);
				if (!s.equals(sb.toString())) {
					System.out.println("failed" + i);
				}
			}
//			if (value == null || value.length != 1000) {
//				int x = 0;
//				x = 20;
//			}
//			if (i == 243) {
//				int x = 30;
//				x = 40;
//			}
		}
		iter.unlock(true);
		CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
//		CacheManager.getInstance().remove("123".getBytes());
		byte[] value = CacheManager.getInstance().get("123".getBytes());
		CacheManager.getInstance().set("123".getBytes(), sb.toString().getBytes());
//		for (i = 100; i < 400; i++) {
//			System.out.println(i);
//			CacheManager.getInstance().remove(new String(""+i).getBytes());
////			if (values == null) {
////				System.out.println("failed: " + i);
////			}
//			
//		}
//		CacheManager.getInstance().set(sb.toString().getBytes(), sb.toString().getBytes());
		byte[] values = CacheManager.getInstance().get("123".getBytes());
		System.out.println("value is: " + values +"done");
		WonderDBCacheService.getInstance().shutdown();
	}
	
	public static class TestRunnable implements Runnable {

		@Override
		public void run() {
			for (int i = 0; i < 1000; i++) {
				int key = Math.abs(random.nextInt()) % 100000;
				int size = Math.abs(random.nextInt()) % 1000;
//				int size = 1000;
				byte[] bytes = new byte[size+1];
				byte[] k = new String(""+key).getBytes();
				Integer x = new Integer(key);
				synchronized (x) {
					CacheManager.getInstance().set(k, bytes);
					map.put(key, bytes.length);
				}
			}
			
		}		
	}
}
