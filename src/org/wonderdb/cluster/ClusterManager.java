package org.wonderdb.cluster;

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

import java.sql.Connection;
import java.util.List;

import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.exception.InvalidIndexException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.IndexKeyType;


public interface ClusterManager {

	public abstract void createCollection(String collectionName, String fileName, boolean isLoggingEnabled,
			List<ColumnNameMeta> columns, String replicaSet) throws InvalidCollectionNameException;

	public abstract void createIndex(String collectionName, String indexName, 
			String storageFile, List<ColumnNameMeta> columns, 
			boolean isUnique, boolean isAsc) throws InvalidIndexException;
	
	public abstract void createReplicaSet(String replicaSetName);

	public abstract void addToReplicaSet(String replicaSetName, String instanceId);

	public abstract void createShard(String collectionName, String rplicaSetName,
			IndexKeyType smallestKey, IndexKeyType maxKey, String indexName) ;

	public abstract boolean isMaster(Shard shard);

	public abstract String addToCluster(String machine);

	public abstract Connection getMasterConnection(Shard shard);

	public abstract List<Shard> getShards(String collectionName);

	public abstract boolean isParticipating(Shard shard);

//	public abstract Shard getShard(String collectionName, ResultContent rc);

	public abstract List<Shard> getShards(String collectionName, AndExpression expList);

//	public abstract int getMasterMachineId(String collectionName, int shardId);
	
	public abstract void createStorage(String fileName, int blockSize, boolean isDefault);

	public abstract String getMachineId();
	
	public void addColumns(String collectionName, List<ColumnNameMeta> list);

	public void init();
	
	public void shutdown();
}