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
package org.wonderdb.query.parse;

import java.util.ArrayList;
import java.util.List;

import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.parser.UQLParser.CreateShardStmt;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.impl.IndexKeyType;


public class CreateShardQuery extends BaseDBQuery {
	String collectionName;
	List<CollectionColumn> idxColumns = new ArrayList<CollectionColumn>();
	CreateShardStmt stmt;
	
	public CreateShardQuery(String query, CreateShardStmt stmt){
		super(query, -1, null);
		this.stmt = stmt;
	}
	
	public void createShard(String collectionName, String indexName, String replicaSetName, IndexKeyType minVal, IndexKeyType maxVal) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		Shard shard = new Shard(colMeta.getSchemaId(), collectionName, replicaSetName);
		SchemaMetadata.getInstance().createShard(shard, indexName, minVal, maxVal);
		ClusterManagerFactory.getInstance().getClusterManager().createShard(collectionName, replicaSetName, minVal, maxVal, indexName);
	}
	
	public void execute() {
		createShard(stmt.collectionName, stmt.indexName, stmt.replicaSetName, null, null);
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	
	public static String getType(String s) {
		if (s.equals("int")) {
			return "is";
		}
		if (s.equals("long")) {
			return "ls";
		}
		if (s.equals("float")) {
			return "fs";
		}
		if (s.equals("double")) {
			return "ds";
		}
		if (s.equals("string")) {
			return "ss";
		}
		throw new RuntimeException("Invalid type");
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
