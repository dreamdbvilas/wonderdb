package org.wonderdb.block.record.manager;

import java.util.HashMap;
import java.util.Map;

import org.wonderdb.cluster.Shard;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IntType;
import org.wonderdb.types.StringType;

public class CacheManager {
	private static CacheManager instance = new CacheManager();
	
	private CacheManager() {
	}
	
	public static CacheManager getInstance() {
		return instance;
	}
	
	public void add(int keyType, String key, int valueType, String value) {
		Map<Integer, DBType> map = new HashMap<>();
		map.put(1, new IntType(keyType));
		map.put(2, new StringType(key));
		map.put(3, new IntType(valueType));
		map.put(4, new StringType(value));
		Shard shard = new Shard("");
		try {
			TableRecordManager.getInstance().addTableRecord("_cache", map, shard);
		} catch (InvalidCollectionNameException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void update() {
//		TableRecordManager.getInstance().updateTableRecord(", recordId, shard, updateSetList, tree, fromMap, txnId)
	}
}
