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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.exception.InvalidCollectionNameException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.parser.jtree.Node;
import org.wonderdb.parser.jtree.QueryEvaluator;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.query.executor.ScatterGatherQueryExecutor;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.types.DBType;
import org.wonderdb.types.DoubleType;
import org.wonderdb.types.FloatType;
import org.wonderdb.types.IntType;
import org.wonderdb.types.LongType;
import org.wonderdb.types.StringType;




public class DBInsertQuery extends BaseDBQuery {
	Map<Integer, DBType> map = new HashMap<Integer, DBType>();
	String collectionName;
	int currentBindPosn = 0;
	AndExpression andExp = null;
	static long count = -1;
	DataContext context = null;
	
	public DBInsertQuery(String q, Node qry, int type, DataContext context, ChannelBuffer buffer) {
		super(q, (SimpleNode) qry, type, buffer);
		this.context = context;
		SimpleNode query = (SimpleNode) qry;
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTTABLENAME);
		if (node != null) {
			collectionName = node.jjtGetFirstToken().image;
		}
		buildMap();
	}
	
	public String getCollenctionName() {
		return collectionName;
	}
	
	public Map<Integer, DBType> getInsertMap() {
		return map;
	}
	
	public AndExpression getExpression() {
		return null;
	}
	
	private void buildMap() {
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(queryNode, UQLParserTreeConstants.JJTINSERTCOLUMNLIST);
		List<SimpleNode> nodeList = new ArrayList<SimpleNode>();
		SimpleNodeHelper.getInstance().getNodes(node, UQLParserTreeConstants.JJTCOLUMNANDALIAS, nodeList);
		Map<String, CollectionAlias> fromMap = new HashMap<String, CollectionAlias>();
		fromMap.put("", new CollectionAlias(collectionName, ""));
		QueryEvaluator qe = new QueryEvaluator(fromMap, context);
		node = SimpleNodeHelper.getInstance().getFirstNode(queryNode, UQLParserTreeConstants.JJTLITERALLIST);
		List<SimpleNode> valueList = new ArrayList<SimpleNode>();
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		SimpleNodeHelper.getInstance().getNodes(node, UQLParserTreeConstants.JJTUNARYEXPRESSION, valueList);
		for (int i = 0; i < nodeList.size(); i++) {
			String colName = nodeList.get(i).jjtGetFirstToken().image;
			SimpleNode value = valueList.get(i);
			qe.processUnaryExpression(value);
			Object val = value.jjtGetValue();
			int colId = colMeta.getColumnId(colName);
			DBType dt = castToDBType(colName, val);
			map.put(colId, dt);
		}
	}
	
	private DBType castToDBType(String colName, Object o) {
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		int type = colMeta.getColumnType(colName);
		if (o instanceof String) {
			return convertToDBType((String) o, type);
		}
		
		if (o instanceof Integer) {
			return convertToDBType( (Integer) o, type);
		}
		
		if (o instanceof Long) {
			return convertToDBType( (Long) o, type);
		}
		
		if (o instanceof Double) {
			return convertToDBType( (Double) o, type);
		}
		
		if (o instanceof Float) {
			return convertToDBType( (Float) o, type);
		}
		
		return null;
	}
	
	public static DBType convertToDBType(String s, int type) {
		switch (type) {
		case SerializerManager.STRING:
			return new StringType(s);
		case SerializerManager.INT:
			return new IntType(Integer.parseInt(s));
		case SerializerManager.LONG:
			return new LongType(Long.parseLong(s));
		case SerializerManager.DOUBLE:
			return new DoubleType(Double.parseDouble(s));
		case SerializerManager.FLOAT:
			return new FloatType(Float.parseFloat(s));
		default:
			return null;
		}
	}

	public static DBType convertToDBType(Object o, int type) {
		if (o == null) {
			return SerializerManager.getInstance().getSerializer(type).getNull(type);
		}
		
		if (o instanceof String) {
			return convertToDBType((String) o, type);
		}
		
		if (o instanceof Integer) {
			return convertToDBType((Integer) o, type);
		}
		
		if (o instanceof Long) {
			return convertToDBType((Long) o, type);
		}
		
		if (o instanceof Double) {
			return convertToDBType((Double) o, type);
		}
		
		if (o instanceof Float) {
			return convertToDBType((Float) o, type);
		}
		
		if (o instanceof StringType) {
			return convertToDBType((StringType) o, type);
		}
		
		if (o instanceof DBType) {
			return (DBType) o;
		}
		
		return null;
	}
	
	public static DBType convertToDBType(StringType dt, int type) {
		return convertToDBType(dt.get(), type);
	}
	
	public static DBType convertToDBType(Integer s, int type) {
		switch (type) {
		case SerializerManager.STRING:
			return new StringType(s != null? s.toString() : null);
		case SerializerManager.INT:
			return new IntType(s);
		case SerializerManager.LONG:
			return new LongType(s != null ? s.longValue() : null);
		case SerializerManager.DOUBLE:
			return new DoubleType(s != null ? s.doubleValue() : null);
		case SerializerManager.FLOAT:
			return new FloatType(s != null ? s.floatValue() : null);
		default:
			return null;
		}
	}

	public static DBType convertToDBType(Long s, int type) {
		switch (type) {
		case SerializerManager.STRING:
			return new StringType(s != null? s.toString() : null);
		case SerializerManager.INT:
			return new IntType(s != null ? s.intValue() : null);
		case SerializerManager.LONG:
			return new LongType(s != null ? s.longValue() : null);
		case SerializerManager.DOUBLE:
			return new DoubleType(s != null ? s.doubleValue() : null);
		case SerializerManager.FLOAT:
			return new FloatType(s != null ? s.floatValue() : null);
		default:
			return null;
		}
	}

	public static DBType convertToDBType(Double s, int type) {
		switch (type) {
		case SerializerManager.STRING:
			return new StringType(s != null? s.toString() : null);
		case SerializerManager.INT:
			return new IntType(s != null ? s.intValue() : null);
		case SerializerManager.LONG:
			return new LongType(s != null ? s.longValue() : null);
		case SerializerManager.DOUBLE:
			return new DoubleType(s != null ? s.doubleValue() : null);
		case SerializerManager.FLOAT:
			return new FloatType(s != null ? s.floatValue() : null);
		default:
			return null;
		}
	}
	
	public static DBType convertToDBType(Float s, int type) {
		switch (type) {
		case SerializerManager.STRING:
			return new StringType(s != null? s.toString() : null);
		case SerializerManager.INT:
			return new IntType(s != null ? s.intValue() : null);
		case SerializerManager.LONG:
			return new LongType(s != null ? s.longValue() : null);
		case SerializerManager.DOUBLE:
			return new DoubleType(s != null ? s.doubleValue() : null);
		case SerializerManager.FLOAT:
			return new FloatType(s != null ? s.floatValue() : null);
		default:
			return null;
		}
	}

	public String getCollectionName() {
		return collectionName;
	}
	
	public int execute() throws InvalidCollectionNameException {
//		SelectStmt s = stmt.values.stmt;
//		List<Map<CollectionAlias, RecordId>> list = null;
//		if (s != null) {
//			DBSelectQuery q = new DBSelectQuery(s, bindParamList);
//			list = q.executeGetDC();
//			for (int i = 0; i < list.size(); i++) {
//				Map<CollectionAlias, RecordId> map = list.get(i);
//				
//			}
//		}
//		
//		return TableRecordManager.getInstance().addTableRecord(collectionName, map);
		ScatterGatherQueryExecutor sgqe = new ScatterGatherQueryExecutor(this);
		return sgqe.insertQuery();
	}
	
//	private StringType getValue(String s) {
//		if (s == null) {
//			return new StringType(null);
//		}
//		s = s.trim();
//		if ("?".equals(s)) {
//			DBType value = bindParamList.get(currentBindPosn++);
//			if (value == null) {
//				return null;
//			}
//			return new StringType(value.toString());
//		}
//		if (s.startsWith("'")) {
//			s = s.replaceFirst("'", "");
//		}
//		if (s.substring(s.length()-1).equals("'")) {
//			s = s.substring(0, s.length()-1);
//		}
//		s = s.replaceAll("\'", "'");
//		return new StringType(s);
//	}	
}
