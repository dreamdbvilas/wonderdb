package org.wonderdb.schema;

import java.util.List;

import org.wonderdb.cluster.ClusterManager;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.types.ColumnNameMeta;

public class ClusterSchemaManager {
	private static ClusterSchemaManager instance = new ClusterSchemaManager();
	public static ClusterSchemaManager getInstance() {
		return instance;
	}
	
	public void createCollection(String collectionName, String storage, List<ColumnNameMeta> idxColumns, 
			boolean shouldCreateClusterObjects) throws InvalidCollectionNameException {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
			ClusterManager clusterManager = ClusterManagerFactory.getInstance().getClusterManager();
			String fileName = storage;
			if (shouldCreateClusterObjects) {
				clusterManager.createCollection(collectionName, fileName, true, idxColumns, null);
			}
			colMeta = SchemaMetadata.getInstance().createNewCollection(collectionName, fileName, idxColumns, 10);		
//			fileName = FileBlockManager.getInstance().getDefaultFileName();
//			idxColumns.clear();
//			idxColumns.add(new CollectionColumn(colMeta, "objectId", "ss"));
//			Index idx;
//			try {
//				idx = new Index  ("objectId"+collectionName, collectionName, idxColumns, true, true);
//				List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(collectionName);
//				for (int i = 0; i < shards.size(); i++) {
//					SchemaMetadata.getInstance().add(idx, shards.get(i));
//				}
//			} catch (InvalidIndexException e) {
//				throw new RuntimeException(e);
//			}

//			if (shouldCreateClusterObjects) {
//				try {
//					clusterManager.createIndex(collectionName, "objectId" + collectionName, fileName, idxColumns, true, true);
//				} catch (InvalidIndexException e) {
//					throw new RuntimeException(e);
//				}
//			}
		} else {
			throw new InvalidCollectionNameException("");
		}
	}
}
