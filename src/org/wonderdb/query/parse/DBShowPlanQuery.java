package org.wonderdb.query.parse;

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

import java.util.List;

import org.wonderdb.cluster.Shard;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree;
import org.wonderdb.query.plan.QueryPlan;



public class DBShowPlanQuery extends BaseDBQuery {
	DBSelectQueryJTree q = null;
	public DBShowPlanQuery(String q1, SimpleNode query) {
		super(q1, query, -1, null);
		q = new DBSelectQueryJTree(q1, query, query, -1, buffer);
	}
	
	public List<QueryPlan> execute() {
		Shard shard = null;
		shard = new Shard("");
		return q.getPlan(shard);
	}

	@Override
	public AndExpression getExpression() {
		return null;
	}
}
