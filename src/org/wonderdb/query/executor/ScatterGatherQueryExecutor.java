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
package org.wonderdb.query.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.block.record.manager.ObjectId;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.block.record.table.TableRecord;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.StaticTableResultContent;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.query.parse.DBDeleteQuery;
import org.wonderdb.query.parse.DBInsertQuery;
import org.wonderdb.query.parse.DBQuery;
import org.wonderdb.query.parse.DBSelectQuery;
import org.wonderdb.query.parse.DBSelectQuery.ResultSetValue;
import org.wonderdb.query.parse.DBUpdateQuery;
import org.wonderdb.query.sql.WonderDBConnection;
import org.wonderdb.query.sql.WonderDBPreparedStatement;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.thread.ThreadPoolExecutorWrapper;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.DoubleType;
import org.wonderdb.types.impl.FloatType;
import org.wonderdb.types.impl.IntType;
import org.wonderdb.types.impl.LongType;
import org.wonderdb.types.impl.StringType;

public class ScatterGatherQueryExecutor {
	static 	ThreadPoolExecutorWrapper executor = new ThreadPoolExecutorWrapper(WonderDBPropertyManager.getInstance().getWriterThreadPoolCoreSize(),
			WonderDBPropertyManager.getInstance().getWriterThreadPoolMaxSize(), 5, 
			WonderDBPropertyManager.getInstance().getWriterThreadPoolQueueSize());
	
			DBQuery query = null;
			
	public ScatterGatherQueryExecutor(DBQuery query) {
		this.query = query;
	}
	
	public static void shutdown() {
		executor.shutdown();
	}
	
	public List<List<ResultSetValue>> selectQuery() {
		List<List<ResultSetValue>> resultListList = new ArrayList<List<ResultSetValue>>();
		AndExpression andExp = query.getExpression();
		String tableName = ((DBSelectQuery) query).getFromList().get(0).getCollectionName();
		List<Shard> shardIds = ClusterManagerFactory.getInstance().getClusterManager().getShards(tableName, andExp);
		List<Future<List<List<ResultSetValue>>>> futures = new ArrayList<Future<List<List<ResultSetValue>>>>(); 
		for (int i = 0; i < shardIds.size(); i++) {
			Shard shard = shardIds.get(i);
			Callable<List<List<ResultSetValue>>> task = null;
			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) || query.executeLocal()) {
				// we should process
				task = new LocalSelectTask(shardIds.get(i), (DBSelectQuery) query);
			} else {
				// process with connection
				task = new RemoteSelectTask((DBSelectQuery) query, shard); 
			}
			@SuppressWarnings("unchecked")
			Future<List<List<ResultSetValue>>> future = (Future<List<List<ResultSetValue>>>) executor.asynchrounousExecute(task);
			futures.add(future);
		}

		for (int i = 0; i < futures.size(); i++) {
			List<List<ResultSetValue>> list;
			try {
				list = futures.get(i).get();
				resultListList.addAll(list);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return resultListList;
	}
	
	public int insertQuery() {
		DBInsertQuery q = (DBInsertQuery) query;
		ObjectId id = new ObjectId(ClusterManagerFactory.getInstance().getClusterManager().getMachineId(), true);
		Map<String, DBType> m = q.getInsertMap();
		m.put("objectId", new StringType(id.toString()));
		Map<ColumnType, DBType> map = TableRecordManager.getInstance().convertTypes(q.getCollectionName(), m);
		TableRecord tr = new TableRecord(map);
		StaticTableResultContent rc = new StaticTableResultContent(tr, null, 1);
		Shard shard = ClusterManagerFactory.getInstance().getClusterManager().getShard(q.getCollectionName(), rc);
		
		Callable<Integer> task = null;
		if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) || query.executeLocal()) {
			// we should process
			task = new LocalInsertTask(shard, map);
		} else {
			// process with connection
			task = new RemoteInsertTask(shard); 
		}
		@SuppressWarnings("unchecked")
		Future<Integer> future = (Future<Integer>) executor.asynchrounousExecute(task);
		try {
			return future.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	public int deleteQuery() {
		DBDeleteQuery q = (DBDeleteQuery) query;
		AndExpression andExp = query.getExpression();
		String tableName = q.getCollection();
		List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(tableName, andExp);
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>(); 
		for (int i = 0; i < shards.size(); i++) {
			Shard shard = shards.get(i);
			Callable<Integer> task = null;
			if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) || query.executeLocal()) {
				// we should process
				task = new LocalDeleteTask(shard);
			} else  {
				// process with connection
				task = new RemoteDeleteTask(shard); 
			}
			@SuppressWarnings("unchecked")
			Future<Integer> future = (Future<Integer>) executor.asynchrounousExecute(task);
			futures.add(future);
		}

		int count = 0;
		for (int i = 0; i < futures.size(); i++) {
			try {
				count = count + futures.get(i).get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}
	
	public int updateQuery() {
		DBUpdateQuery q = (DBUpdateQuery) query;
		List<Shard> shards = ClusterManagerFactory.getInstance().getClusterManager().getShards(q.getCollectionName(), q.getExpression());
		if (shards == null || shards.size() > 1) {
			throw new RuntimeException("Invalid update stmt");
		}
		Shard shard = shards.get(0);
		Callable<Integer> task = null;
		if (ClusterManagerFactory.getInstance().getClusterManager().isMaster(shard) || query.executeLocal()) {
			// we should process
			task = new LocalUpdateTask(q, shard);
		} else  {
			// process with connection
			task = new RemoteInsertTask(shard); 
		}
		@SuppressWarnings("unchecked")
		Future<Integer> future = (Future<Integer>) executor.asynchrounousExecute(task);
		try {
			return future.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	private List<List<ResultSetValue>> convert(ResultSet rs) {
		List<List<ResultSetValue>> resultListList = new ArrayList<List<ResultSetValue>>();
		try {
			ResultSetMetaData rsMeta = rs.getMetaData();
			while (rs.next()) {
				List<ResultSetValue> list = new ArrayList<DBSelectQuery.ResultSetValue>();
				for (int i = 0; i < rsMeta.getColumnCount(); i++) {
					ResultSetValue rsv = new ResultSetValue();
					list.add(rsv);
					int type = rsMeta.getColumnType(i);
					rsv.columnName = rsMeta.getColumnName(i);
					
					switch (type) {
					case Types.VARCHAR: 
						String s = rs.getString(i);
						if (s != null) {
							rsv.value = new StringType(s);
						}
						break;
					case Types.NUMERIC:
						Long l = rs.getLong(i);
						if (l != null) {
							rsv.value = new LongType(l);
						}
						break;
					case Types.DOUBLE:
						Double d = rs.getDouble(i);
						if (d != null) {
							rsv.value = new DoubleType(d);
						}
						break;
					case Types.INTEGER:
						Integer iv = rs.getInt(i);
						if (iv != null) {
							rsv.value = new IntType(iv);
						}
						break;
					case Types.FLOAT:
						Float f = rs.getFloat(i);
						if (f != null) {
							rsv.value = new FloatType(f);
						}
						break;
					}
				}
				resultListList.add(list);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return resultListList;
	}
	
	
	public int executeUpdate() {
		return -1;
	}
	
	public class RemoteSelectTask implements Callable<List<List<ResultSetValue>>> {
		Connection connection = null;
		Shard shard = null;
		DBSelectQuery query = null;
		public RemoteSelectTask(DBSelectQuery query, Shard shard) {
			this.shard = shard;
			this.query = query;
		}
		
		@Override
		public List<List<ResultSetValue>> call() throws Exception {
			WonderDBConnection connection = (WonderDBConnection) ClusterManagerFactory.getInstance().getClusterManager().getMasterConnection(shard);
			try {
				WonderDBPreparedStatement stmt = (WonderDBPreparedStatement) connection.prepareStatement(query.getQuery());
				ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
				buffer.writeInt(query.getRawBuffer().capacity());
				buffer.clear();
				buffer.writerIndex(4);
				query.getRawBuffer().clear();
				query.getRawBuffer().writerIndex(query.getRawBuffer().capacity());
				ChannelBuffer buf = ChannelBuffers.wrappedBuffer(buffer, query.getRawBuffer());
				return convert(stmt.executeQuery(buf));
			} finally {
				connection.close();
			}
		}
	}
	
	public class LocalSelectTask implements Callable<List<List<ResultSetValue>>> {
		DBSelectQuery query = null;
		Shard shard = null;
		
		public LocalSelectTask(Shard shard, DBSelectQuery query) {
			this.query = query;
			this.shard = shard;
		}
		
		@Override
		public List<List<ResultSetValue>> call() throws Exception {
			return query.executeLocal(shard);
		}		
	}
	
	public class LocalInsertTask implements Callable<Integer> {
		Map<ColumnType, DBType> map = null;
		Shard shard = null;
		public LocalInsertTask(Shard shard, Map<ColumnType, DBType> map) {
			this.map = map;
			this.shard = shard;
		}
		
		@Override
		public Integer call() throws Exception {
			DBInsertQuery q = (DBInsertQuery) query;
			return TableRecordManager.getInstance().addTableRecord(q.getCollectionName(), map, shard);
		}
	}
	
	public class RemoteInsertTask implements Callable<Integer> {
		Connection connection = null;
		Shard shard = null;
		
		public RemoteInsertTask(Shard shard) {
			this.shard = shard;
		}
		
		@Override
		public Integer call() throws Exception {
			WonderDBConnection connection = null;
			try {
				connection = (WonderDBConnection) ClusterManagerFactory.getInstance().getClusterManager().getMasterConnection(shard);
				WonderDBPreparedStatement stmt = (WonderDBPreparedStatement) connection.prepareStatement(query.getQueryString());
				ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
				buffer.writeInt(query.getRawBuffer().capacity());
				buffer.clear();
				buffer.writerIndex(4);
				query.getRawBuffer().clear();
				query.getRawBuffer().writerIndex(query.getRawBuffer().capacity());
				ChannelBuffer buf = ChannelBuffers.wrappedBuffer(buffer, query.getRawBuffer());
				return stmt.executeUpdate(buf);
			} finally {
				connection.close();
			}
		}
	}
	
	public class LocalDeleteTask implements Callable<Integer> {
		Shard shard = null;
		
		public LocalDeleteTask(Shard shard) {
			this.shard = shard;
		}
		
		@Override
		public Integer call() throws Exception {
			// TODO Auto-generated method stub
			return ((DBDeleteQuery) query).execute(shard);
		}
	}
	
	public class RemoteDeleteTask implements Callable<Integer> {
		Shard shard = null;
		public RemoteDeleteTask(Shard shard) {
			this.shard = shard;
		}
		
		@Override
		public Integer call() throws Exception {
			WonderDBConnection connection = null;
			try {
				connection = (WonderDBConnection) ClusterManagerFactory.getInstance().getClusterManager().getMasterConnection(shard);
				WonderDBPreparedStatement stmt = (WonderDBPreparedStatement) connection.prepareStatement(query.getQueryString());
				ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
				buffer.writeInt(query.getRawBuffer().capacity());
				buffer.clear();
				buffer.writerIndex(4);
				query.getRawBuffer().clear();
				query.getRawBuffer().writerIndex(query.getRawBuffer().capacity());
				ChannelBuffer buf = ChannelBuffers.wrappedBuffer(buffer, query.getRawBuffer());
				return stmt.executeUpdate(buf);
			} finally {
				connection.close();
			}
		}
	}
	
	public class RemoteUpdateTask implements Callable<Integer> {
		Shard shard = null;
		public RemoteUpdateTask(Shard shard) {
			this.shard = shard;
		}
		
		@Override
		public Integer call() throws Exception {
			WonderDBConnection connection = null;
			try {
				connection = (WonderDBConnection) ClusterManagerFactory.getInstance().getClusterManager().getMasterConnection(shard);
				WonderDBPreparedStatement stmt = (WonderDBPreparedStatement) connection.prepareStatement(query.getQueryString());
				ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
				buffer.writeInt(query.getRawBuffer().capacity());
				buffer.clear();
				buffer.writerIndex(4);
				query.getRawBuffer().clear();
				query.getRawBuffer().writerIndex(query.getRawBuffer().capacity());
				ChannelBuffer buf = ChannelBuffers.wrappedBuffer(buffer, query.getRawBuffer());
				return stmt.executeUpdate(buf);			
			} finally {
				connection.close();
			}
		}
	}
	
	public class LocalUpdateTask implements Callable<Integer> {
		Shard shard = null;
		DBUpdateQuery query = null;
		
		public LocalUpdateTask(DBUpdateQuery query, Shard shard) {
			this.shard = shard;
			this.query = query;
		}
		
		@Override
		public Integer call() throws Exception {
			return query.execute(shard);
		}
	}
}
