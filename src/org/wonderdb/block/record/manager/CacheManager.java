package org.wonderdb.block.record.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.ByteArrayType;
import org.wonderdb.types.DBType;
import org.wonderdb.types.ExtendedColumn;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;
import org.wonderdb.types.record.TableRecord;

public class CacheManager {
	
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
	
	public int set(byte[] key, byte[] value) {
		Map<Integer, DBType> map = new HashMap<Integer, DBType>();
		map.put(0, new ByteArrayType(key));
		map.put(1, new ByteArrayType(value));
		Shard shard = new Shard("");
		try {
			return TableRecordManager.getInstance().addTableRecord("cache", map, shard, true);
		} catch (InvalidCollectionNameException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	public void remove(byte[] key) {
		TransactionId txnId = null;
		Set<Object> pinnedBlocks = new HashSet<Object>();
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
}
