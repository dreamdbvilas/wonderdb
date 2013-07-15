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

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.cluster.Shard;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.query.plan.QueryPlan;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;



public class DBShowPlanQuery extends BaseDBQuery {
	DBSelectQuery q = null;
	public DBShowPlanQuery(String query, List<DBType> bindParamList) {
		super(query, -1, null);
		q = (DBSelectQuery) parse(query, bindParamList, null);
	}
	
	public DBQuery parse(String query, List<DBType> bindParamList, ChannelBuffer buffer) {		
		String s = query.replace("explain plan", "").trim();
		return QueryParser.getInstance().parse(s, bindParamList, -1, buffer);
	}
	
	public List<QueryPlan> execute() {
		Shard shard = null;
		String collectionName = q.getFromList().get(0).getCollectionName();
		int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
		shard = new Shard(schemaId, collectionName, collectionName);
		return q.getPlan(shard);
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
