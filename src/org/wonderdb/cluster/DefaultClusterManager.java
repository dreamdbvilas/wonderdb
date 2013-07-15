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
package org.wonderdb.cluster;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.collection.exceptions.InvalidIndexException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.impl.IndexKeyType;


public class DefaultClusterManager implements ClusterManager {
	String machineId = "0";
	
	public DefaultClusterManager() {
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#createCollection(java.lang.String, java.lang.String)
	 */
	@Override
	public void createCollection(String collectionName, String fileName, boolean isLoggingEnabled,
			List<CollectionColumn> columns, String replicaSet) throws InvalidCollectionNameException {
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#createReplicaSet(java.lang.String)
	 */
	@Override
	public void createReplicaSet(String replicaSetName) {
		
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#addToReplicaSet(java.lang.String, int)
	 */
	@Override
	public void addToReplicaSet(String replicaSetName, String instanceId) {
	}

	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#createShard(java.lang.String, java.lang.String, org.wonderdb.types.impl.IndexKeyType)
	 */
	@Override
	public void createShard(String collectionName, String rplicaSetName,
			IndexKeyType smallestKey, IndexKeyType maxKey, String indexName)  {
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#isMaster(int)
	 */
	@Override
	public boolean isMaster(Shard shard) {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#addToCluster(java.lang.String, int)
	 */
	@Override
	public String addToCluster(String machine) {
		return "-1";
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#getMasterConnection(int)
	 */
	@Override
	public Connection getMasterConnection(Shard shard) {
		return null;
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#getShards(java.lang.String)
	 */
	@Override
	public List<Shard> getShards(String collectionName) {
		List<Shard> shards = new ArrayList<Shard>();
		int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
		Shard shard = new Shard(schemaId, collectionName, collectionName);
		shards.add(shard);
		return shards;
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#isParticipating(int)
	 */
	@Override
	public boolean isParticipating(Shard shard) {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#getShardId(org.wonderdb.block.record.table.TableRecord)
	 */
	@Override
	public Shard getShard(String collectionName, ResultContent rc) {
		int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
		return new Shard(schemaId, collectionName, collectionName);
	}
	
	/* (non-Javadoc)
	 * @see org.wonderdb.cluster.ClusterManager#getShards(org.wonderdb.expression.AndExpression)
	 */
	@Override
	public List<Shard> getShards(String collectionName, AndExpression expList) {
		int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
		List<Shard> shards = new ArrayList<Shard>();
		shards.add(new Shard(schemaId, collectionName, collectionName));
		return shards;
	}
	
	public String getMachineId() {
		return machineId;
	}
	
	public void setMachineId(String id) {
		machineId = id;
	}
	
	@Override
	public void createStorage(String fileName, int blockSize, boolean isDefault) {
	}

	@Override
	public void createIndex(String collectionName, String indexName, 
			String storageFile, List<CollectionColumn> columns, 
			boolean isUnique, boolean isAsc) throws InvalidIndexException {
	}

	@Override
	public void init() {
	}

	@Override
	public void addColumns(String collectionName, List<CollectionColumn> list) {
	}

	@Override
	public void shutdown() {
	}
}
