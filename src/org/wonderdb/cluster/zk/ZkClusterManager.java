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
package org.wonderdb.cluster.zk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cluster.ClusterManager;
import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.cluster.DefaultClusterManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.cluster.kafka.KafkaConsumerManager;
import org.wonderdb.cluster.kafka.KafkaProducerManager;
import org.wonderdb.collection.ResultContent;
import org.wonderdb.collection.exceptions.InvalidCollectionNameException;
import org.wonderdb.collection.exceptions.InvalidIndexException;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.StaticOperand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.ClusterSchemaManager;
import org.wonderdb.schema.CollectionColumn;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.FileBlockEntryType;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.IndexMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.seralizers.IndexSerializer;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;
import org.wonderdb.types.impl.StringLikeType;
import org.wonderdb.types.impl.StringType;

public class ZkClusterManager implements ClusterManager {
	private static final String REPLICASET_PATH = "/WonderDB/replicaSets";
	private static final String CLUSTER_NODES = "/WonderDB/clusterNodes";
	private static final String CURATOR_REPLICASET_LEADER = "/WonderDB/curator/leaders/replicaSets";
	private static final String COLLECTIONS_PATH = "/WonderDB/collections";
	private static final String STORAGE_PATH = "/WonderDB/storages";
	
	ConcurrentMap<String, LeaderSelector> replicaSetParticipation = new ConcurrentHashMap<String, LeaderSelector>();
	ConcurrentMap<String, String> replicaSetLeaders = new ConcurrentHashMap<String, String>();
	ConcurrentMap<String, String> machineIdTNameMap = new ConcurrentHashMap<String, String>();
	ConcurrentMap<String, Set<Shard>> collectionNameByShardMap = new ConcurrentHashMap<String, Set<Shard>>();
	ConcurrentMap<String, Shard> replicaSetShardMap = new ConcurrentHashMap<String, Shard>();
//	ConcurrentMap<String, ZkIndex> indexMap = new ConcurrentHashMap<String, ZkIndex>();
	CuratorWatcher watcher = null;
//	ConcurrentMap<String, ZkCollection> collectionMap = new ConcurrentHashMap<String, ZkClusterManager.ZkCollection>();
	private String nodeId = null;
	
	CuratorFramework curatorFramework = null;
	Watcher w = null;
	ClusterManager defaultCM = new DefaultClusterManager();
	LinkedBlockingQueue<WatchedEvent> zkEventQueue = new LinkedBlockingQueue<WatchedEvent>();
	Thread zkProcessotrThread = null;
	
	public ZkClusterManager() {
	}
	
	public void init() {
		String zkConnectString = WonderDBPropertyManager.getInstance().getZkConnString();
		int connectRetryCount = WonderDBPropertyManager.getInstance().getZkConnectRetryCount();
		int sleepTime = WonderDBPropertyManager.getInstance().getZkConnectRetrySleepTime();
		int connectionTimeout = WonderDBPropertyManager.getInstance().getZkConnectionTimeout();
		int sessionTimeout = WonderDBPropertyManager.getInstance().getZkSessionTimeout();
		RetryNTimes rnt = new RetryNTimes(connectRetryCount, sleepTime);

		curatorFramework = CuratorFrameworkFactory.builder()
		.connectString(zkConnectString)
		.retryPolicy(rnt)
		.connectionTimeoutMs(connectionTimeout)
		.sessionTimeoutMs(sessionTimeout)
		.build();
	curatorFramework.start();

	watcher = new ChangeCommandWatcher();
	ZkEventProcessor zkEventProcesstor = new ZkEventProcessor();
	zkProcessotrThread = new Thread(zkEventProcesstor);
	zkProcessotrThread.start();
	create(STORAGE_PATH);
	create(COLLECTIONS_PATH);
	create(CLUSTER_NODES);
	create(REPLICASET_PATH);
		readNodeId();
		((DefaultClusterManager) defaultCM).setMachineId(nodeId);
		((DefaultClusterManager) defaultCM).init();
		readStorages();
		readReplicaSets();
		readClusterNodes();
		readCollections();
	}
	
	@Override
	public void shutdown() {
		Iterator<LeaderSelector> iter = replicaSetParticipation.values().iterator();
		while (iter.hasNext()) {
			iter.next().close();
		}
		zkProcessotrThread.interrupt();
		curatorFramework.close();
	}
	
	private void readNodeId() {
		String fileName = "nodeId";
		File file = new File(fileName);
		try {
			InetAddress address = Inet6Address.getLocalHost();
			String machine = address.getCanonicalHostName();
			int port = WonderDBPropertyManager.getInstance().getServerPort();
			if (file.exists()) {
				FileInputStream fis = new FileInputStream(file);
				ByteBuffer buffer = ByteBuffer.allocate(100);
				fis.getChannel().read(buffer);
				buffer.clear();
				ChannelBuffer cb = ChannelBuffers.wrappedBuffer(buffer);
				cb.clear();
				cb.writerIndex(cb.capacity());
				int len = cb.readInt();
				byte[] bytes = new byte[len];
				cb.readBytes(bytes);
				nodeId = new String(bytes);
				fis.close();
			} else {
				
				nodeId = addToCluster("wonderdb://" + machine + ":" + port);
				
				ChannelBuffer cb = ChannelBuffers.buffer(100);
				cb.writeInt(nodeId.getBytes().length);
				cb.writeBytes(nodeId.getBytes());
				FileOutputStream fos = new FileOutputStream(file);
				ByteBuffer buffer = ByteBuffer.allocate(100);
				buffer.put(cb.array());
				buffer.flip();
				fos.getChannel().write(buffer);
				fos.close();
			}
			registerNodeId("wonderdb://" + machine + ":" + port);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
		
	private void registerNodeId(String val) {
		String path = CLUSTER_NODES + "/" + nodeId;
		try {
			curatorFramework.create().forPath(path, val.getBytes());
		} catch (Exception e) {
			try {
				curatorFramework.setData().forPath(path, val.getBytes());
			} catch (Exception e1) {
				throw new RuntimeException(e1);
			}
		}
	}
	
	@Override
	public void createCollection(String collectionName, String fileName, boolean isLoggingEnabled,
			List<CollectionColumn> columns, String replicaSet) throws InvalidCollectionNameException {
		String path = COLLECTIONS_PATH + "/" + collectionName;
		try {
			curatorFramework.create().creatingParentsIfNeeded().forPath(path);
			String loggingEnabled = "false";
			if (isLoggingEnabled) {
				loggingEnabled = "true";
			}
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/" + "isLoggingEnabled", loggingEnabled.getBytes());
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/" + "storage", fileName.getBytes());
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/" + "indexes");
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/" + "shards");
			addColumns(collectionName, columns);			
			curatorFramework.setData().forPath(COLLECTIONS_PATH);
		} catch (Exception e) {
			if (e instanceof NodeExistsException) {
				throw new InvalidCollectionNameException(collectionName);
			} else {
				throw new RuntimeException(e);
			}
		}		
	}
	
	public void addColumns(String collectionName, List<CollectionColumn> list) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/columns";
		for (int i = 0; i < list.size(); i++) {
			addColumn(path, list.get(i));
		}
		try {
			curatorFramework.setData().forPath(COLLECTIONS_PATH + "/" + collectionName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void addColumn(String path, CollectionColumn cc) {
		try {
			String p = path + "/" + cc.getColumnName();
			curatorFramework.create().creatingParentsIfNeeded().forPath(p);
			p = path + "/" + cc.getColumnName() + "/serializerName";
			curatorFramework.create().creatingParentsIfNeeded().forPath(p, cc.getCollectionColumnSerializerName().getBytes());
			p = path + "/" + cc.getColumnName() + "/isNullable";
			curatorFramework.create().creatingParentsIfNeeded().forPath(p, cc.isNullable() == true ? "true".getBytes() : "false".getBytes());
			p = path + "/" + cc.getColumnName() + "/isQueriable";
			curatorFramework.create().creatingParentsIfNeeded().forPath(p, cc.isQueriable() == true ? "true".getBytes() : "false".getBytes());
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createReplicaSet(String replicaSetName)  {
		String path = REPLICASET_PATH + "/" + replicaSetName;
		String leaderPath = path + "/leader";
		try {
			curatorFramework.create().creatingParentsIfNeeded().forPath(path);
			curatorFramework.create().creatingParentsIfNeeded().forPath(leaderPath);
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/nodes");
			curatorFramework.getChildren().usingWatcher(watcher).forPath(path + "/nodes");
			curatorFramework.getData().usingWatcher(watcher).forPath(leaderPath);
		} catch (Exception e) {
			throw new RuntimeException("ZK couldn't create replica set: " + replicaSetName);
		}
	}

	@Override
	public void addToReplicaSet(String replicaSetName, String instanceId) {
		String path = REPLICASET_PATH + "/" + replicaSetName + "/nodes/" + instanceId;
		create(path);
	}

	@Override
	public void createShard(String collectionName, String replicaSetName,
			IndexKeyType smallestKey, IndexKeyType maxKey, String indexName) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/shards";
		
		int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
		String s = indexName != null ? indexName : "";
		Shard shard = new Shard(schemaId, collectionName, replicaSetName);
		Set<Shard> shards = collectionNameByShardMap.get(collectionName);
		if (shards == null) {
			shards = new HashSet<Shard>();
			collectionNameByShardMap.put(collectionName, shards);
		}
		shards.add(shard);
		replicaSetShardMap.put(replicaSetName, shard);
		try {
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/indexName", s.getBytes());
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			int indexId = SchemaMetadata.getInstance().getIndex(indexName).getSchemaId();
			byte[] bytes = null;
			if (smallestKey != null) {
				IndexSerializer.getInstance().toBytes(smallestKey, buffer, indexId);
				bytes = buffer.array();
			}
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/" + replicaSetName + "/minIndexVal", bytes);
			if (maxKey != null) {
				IndexSerializer.getInstance().toBytes(maxKey, buffer, indexId);
				bytes = buffer.array();				
			}
			curatorFramework.create().creatingParentsIfNeeded().forPath(path + "/" + replicaSetName + "/maxIndexVal", bytes);
			curatorFramework.setData().forPath(COLLECTIONS_PATH + "/" + collectionName, null);
			Set<Shard> set = collectionNameByShardMap.get(collectionName);
			shard = new Shard(-1, collectionName, collectionName);
			set.remove(shard);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean isMaster(Shard shard) {
		if (replicaSetLeaders.size() != 0) {
			String machineId = replicaSetLeaders.get(shard.getReplicaSetName());
			return machineId == null ? true : machineId.equals(getMachineId());
		}
		return true;
	}

	@Override
	public String addToCluster(String machine) {
		String path = CLUSTER_NODES;
		String s = null;
		try {
			s = curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path+"/", machine.getBytes());
		} catch (Exception e) {
			throw new RuntimeException("Couldn't add to cluster: " + machine);
		}
		
		return getPosition(s, 0);
	}

	@Override
	public Connection getMasterConnection(Shard shard) {
		String machineId = replicaSetLeaders.get(shard.getReplicaSetName());
		String connectStr = machineIdTNameMap.get(machineId);
		try {
			return DriverManager.getConnection(connectStr);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Shard> getShards(String collectionName) {
		Set<Shard> list = collectionNameByShardMap.get(collectionName);
		if (list == null || list.size() == 0) {
			return defaultCM.getShards(collectionName);
//			collectionNameByShardMap.put(collectionName, list);
		}
		return new ArrayList<Shard>(list);
	}

	@Override
	public boolean isParticipating(Shard shard) {
		if (shard.getSchemaId() < 3 || shard.getSchemaObjectName().equals(shard.getReplicaSetName())) {
			return true;
		}
		return replicaSetParticipation.containsKey(shard.getReplicaSetName());
	}

	@SuppressWarnings("unused")
	@Override
	public Shard getShard(String collectionName, ResultContent rc) {
		List<Shard> list = getShards(collectionName);
		if (list == null || list.size() == 0) {
			return defaultCM.getShard(collectionName, rc);
		}
		if (list.size() == 1) {
			return list.get(0);
		}
		
		for (int x = 0; x < list.size(); x++) {
			Shard shard = list.get(x);
			Map<ColumnType, DBType> minMap = shard.getMinMap();
			Map<ColumnType, DBType> maxMap = shard.getMaxMap();
			List<ColumnType> ctList = null;
			DBType minVal = null;
			DBType maxVal = null;
			
			if (minMap != null) {
				ctList = new ArrayList<ColumnType>(minMap.keySet());
			} else if (maxMap != null) {
				ctList = new ArrayList<ColumnType>(maxMap.keySet());
			}
			
			if (ctList != null) {
				for (int j = 0; j < ctList.size(); j++) {
					ColumnType ct = ctList.get(j);
					DBType dbt = rc.getValue(ct, null);
					if (minMap != null) {
						minVal = minMap.get(ct);
					}
					if (maxMap != null) {
						maxVal = maxMap.get(ct);
					}
					if (!compare(dbt, minVal, maxVal)) {
						continue;
					}
				}
			}
			return shard;
		}
		return null;
	}
	
	@Override
	public List<Shard> getShards(String collectionName, AndExpression expList) {
		List<Shard> list = getShards(collectionName);
		if (list == null || list.size() == 0) {
			return defaultCM.getShards(collectionName, expList);
		}

		List<Shard> retList = new ArrayList<Shard>();
		if (list.size() == 1) {
			return list;
		}
		
		for (int i = 0; i < list.size(); i++) {
			Shard shard = list.get(i);
			if (evaluate(expList, shard)) {
				retList.add(shard);
			}
			
		}
		return retList;
	}
	
	private void readStorages() {
		String path = STORAGE_PATH;
		
		List<String> storages = null;
		create(path);
		try {
			curatorFramework.getData().usingWatcher(watcher).forPath(path);
			storages = curatorFramework.getChildren().forPath(path);
			for (int i = 0; i < storages.size(); i++) {
				String s = storages.get(i);
				readStorage(s);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void readStorage(String fileName) throws Exception {
		String path = STORAGE_PATH;
		byte[] bytes = curatorFramework.getData().forPath(path + "/" + fileName);
		bytes = curatorFramework.getData().forPath(path + "/" + fileName + "/blockSize");
		String bs = new String(bytes);
		int blockSize = Integer.parseInt(bs);
		bytes = curatorFramework.getData().forPath(path + "/" + fileName + "/default");
		String defaultStorage = new String(bytes);
		boolean isDefault = false;
		if ("true".equals(defaultStorage)) {
			isDefault = true;
		}
		FileBlockEntryType entry = FileBlockManager.getInstance().getMetaEntry(fileName);
		if (entry == null) {
			SchemaMetadata.getInstance().addStorage(fileName, blockSize, isDefault);
		}
	}
	
	private boolean evaluate(AndExpression andExp, Shard shard) {
		if (andExp == null) {
			return true;
		}
		
		List<BasicExpression> list = andExp.getExpList();
		if (list == null) {
			return true;
		}
		
		for (int i = 0; i < list.size(); i++) {
			BasicExpression exp = list.get(i);
			if (evaluate(exp, shard)) {
				continue;
			} else {
				return false;
			}
		}
		
		return true;
	}
	
	private boolean evaluate(BasicExpression exp, Shard shard) {
		Operand l = exp.getLeftOperand();
		Operand r = exp.getRightOperand();
		Map<ColumnType, DBType> minMap = shard.getMinMap();
		Map<ColumnType, DBType> maxMap = shard.getMaxMap();
		
		DBType minVal = null;
		DBType maxVal = null;
		
		ColumnType ct = null;

		if (l instanceof VariableOperand && r instanceof StaticOperand) {
			VariableOperand vo = (VariableOperand) l;
			StaticOperand so = (StaticOperand) r;
			if (minMap != null) {
				minVal = minMap.get(vo.getColumnType());
				ct = vo.getColumnType();
			}

			if (maxMap != null) {
				maxVal = maxMap.get(vo.getColumnType());
				ct = vo.getColumnType();
			}
			
			if (ct != null) {
				return compare(so.getValue(null), minVal, maxVal);
			}
		}
		
		if (l instanceof StaticOperand && r instanceof VariableOperand) {
			VariableOperand vo = (VariableOperand) r;
			StaticOperand so = (StaticOperand) l;
			
			if (minMap != null) {
				minVal = minMap.get(vo.getColumnType());
				ct = vo.getColumnType();
			}

			if (minMap != null) {
				maxVal = maxMap.get(vo.getColumnType());
				ct = vo.getColumnType();
			}
			
			if (minMap.containsKey(vo.getColumnType())) {
				return compare(so.getValue(null), minVal, maxVal);
			}
			return false;
		}
		
		return true;
		
	}
	
	private boolean compare(DBType expCompareVal, DBType minVal, DBType maxVal) {
		DBType mnVal = minVal;
		DBType mxVal = maxVal;
		
		if (minVal instanceof StringType) {
			mnVal = new StringLikeType(((StringType) minVal).get());		
		}
		
		if (maxVal instanceof StringType) {
			mxVal = new StringLikeType(((StringType) maxVal).get());
		}
		
		if (mnVal != null) {
			int minCV = mnVal.compareTo(expCompareVal);
			if (minCV > 0) {
				return false;
			}
		}
		if (mxVal != null) {
			int maxCV = mxVal.compareTo(expCompareVal);
			if (maxCV <= 0) {
				return false;
			}
		}
		return true;
	}
	
	private class ZkEventProcessor implements Runnable {

		@Override
		public void run() {
			while (true) {
				WatchedEvent event = null;
				try {
					event = zkEventQueue.take();
					process(event);
				} catch (InterruptedException e) {
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
		
		public void process(WatchedEvent event) throws Exception {
			if (event == null || event.getPath() == null) {
				return;
			}
			String s = event.getPath();
			if (s.startsWith(REPLICASET_PATH)) {
				handleReplicaSetChange(event);
			}
			
			if (s.startsWith(COLLECTIONS_PATH)) {
				if (s.endsWith("indexes")) {
					handleIndexChange(event);
				} else {
					handleCollectionChange(event);
				}
			}
			
			if (s.startsWith(STORAGE_PATH)) {
				handleStorageChange(event);
			}
			
			if (s.startsWith(CLUSTER_NODES)) {
				handleClusterNodes(event);
			}
			
			if (s.endsWith("shards")) {
				String collectionName = getPosition(s, 1);
				readShards(collectionName);
			}
		}

		private void handleClusterNodes(WatchedEvent event) {
			String s = event.getPath();
			if (EventType.NodeChildrenChanged == event.getType()) {
				readClusterNodes();
			} 
			
			if (EventType.NodeDataChanged == event.getType()) {
				String node = getPosition(s, 0);
				readClusterNode(node);
			}
		}
		
		private void handleStorageChange(WatchedEvent event) {
			if (EventType.NodeDataChanged == event.getType()) {
				readStorages();
			}
		}
		
		private void handleReplicaSetChange(WatchedEvent event) {
			String s = event.getPath();
			if (EventType.NodeChildrenChanged == event.getType()) {
				String n = getPosition(s, 0);
				if ("nodes".equals(n)) {
					// new node added to replicaset
					String replicaSetName = getPosition(s, 1);
					readReplicaSet(replicaSetName);
				} else {
					// new replica set
					readReplicaSets();
				}
			}
			
			if (EventType.NodeDataChanged == event.getType()) {
				String replicaSetName = getPosition(s, 1);
				readReplicaSetLeader(replicaSetName);
			}
		}			

		private void handleIndexChange(WatchedEvent event) {
			String s = event.getPath();
			if (EventType.NodeDataChanged == event.getType()) {
				String collectionName = getPosition(s, 1);
				readIndexes(collectionName);
			}			
		}
		
		private void handleCollectionChange(WatchedEvent event) {
			String s = event.getPath();
//			if (EventType.NodeChildrenChanged == event.getType()) {
//				readCollections();
//			}
//			
			if (EventType.NodeDataChanged == event.getType()) {
				if (COLLECTIONS_PATH.equals(s)) {
					readCollections();
				} else {
					String collectionName = getPosition(s, 0);
					readCollection(collectionName);
				}
			}
		}		
	}
	
	private class ChangeCommandWatcher implements CuratorWatcher {

		@Override
		public void process(WatchedEvent event) throws Exception {
			zkEventQueue.put(event);
		}
		
	}
	
	private class ReplicaSetLeaderListener implements LeaderSelectorListener {
		String replicaSetName = null;
		Object lockObj = new Object();
		
		ReplicaSetLeaderListener(String replicaSetName) {
			this.replicaSetName = replicaSetName;
		}
		
		@Override
		public void stateChanged(CuratorFramework arg0, ConnectionState arg1) {
			if (ConnectionState.LOST  == arg1 || ConnectionState.SUSPENDED == arg1) {
				replicaSetLeaders.remove(replicaSetName);
				KafkaProducerManager.getInstance().removeProducer(replicaSetName);
				KafkaConsumerManager.getInstance().startConsumer(replicaSetName);
				synchronized (lockObj) {
					lockObj.notifyAll();
				}
			} else if (ConnectionState.CONNECTED == arg1 || ConnectionState.RECONNECTED == arg1) {
				init();
			}
		}

		@Override
		public void takeLeadership(CuratorFramework arg0) throws Exception {
			String path = REPLICASET_PATH + "/" + replicaSetName +"/leader";
			String s = String.valueOf(nodeId);
			curatorFramework.setData().forPath(path, s.getBytes());
			replicaSetLeaders.put(replicaSetName, nodeId);
			KafkaProducerManager.getInstance().createProducer(replicaSetName);
			KafkaConsumerManager.getInstance().removeConsumer(replicaSetName);
			while (true) {
				synchronized (lockObj) {
					lockObj.wait();
					break;
				}
			}
			
		}
	}

	private String getPosition(String node, int posn) {
		String[] split = node.split("/");
		if (split == null || split.length <= posn) {
			return null;
		}
		
		return split[split.length-1-posn];
	}
	
	private void readReplicaSets() {
		List<String> list = null;
		try {
			create(REPLICASET_PATH);
			create(CURATOR_REPLICASET_LEADER);
			list = curatorFramework.getChildren().usingWatcher(watcher).forPath(REPLICASET_PATH);
		} catch (Exception e) {
			throw new RuntimeException();
		}

		for (int i = 0; i < list.size(); i++) {
			String replicaSet = list.get(i);
			readReplicaSet(replicaSet);
		}	


	}
		
	private void readReplicaSet(String replicaSet) {
		readReplicaSetNodes(replicaSet);
		readReplicaSetLeader(replicaSet);
	}
		
	private void readReplicaSetNodes(String replicaSet) {
		List<String> rList = null;
		try {
			create(REPLICASET_PATH + "/" + replicaSet + "/nodes");
			rList = curatorFramework.getChildren().usingWatcher(watcher).forPath(REPLICASET_PATH + "/" + replicaSet + "/nodes");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		boolean participating = false;
		
		for (int j = 0; j < rList.size(); j++) {
			String rn = rList.get(j);
			if (nodeId.equals(rn)) {
				participating = true;
			}
		}				

		if (participating) {
			LeaderSelector leaderSelector = replicaSetParticipation.get(replicaSet);
			if (leaderSelector == null) {
				ReplicaSetLeaderListener leaderListener = new ReplicaSetLeaderListener(replicaSet);
				leaderSelector = new LeaderSelector(curatorFramework, CURATOR_REPLICASET_LEADER + "/" + replicaSet, leaderListener);
				leaderSelector.start();
				replicaSetParticipation.put(replicaSet, leaderSelector);
			}
		}
	}
	
	private void readReplicaSetLeader(String replicaSet) {
		byte[] bytes = null;
		try {
			create(REPLICASET_PATH + "/" + replicaSet + "/leader");
			bytes = curatorFramework.getData().usingWatcher(watcher).forPath(REPLICASET_PATH + "/" + replicaSet + "/leader");
			if (bytes != null && bytes.length > 0) {
				String leader = new String(bytes);
				if (!leader.equals(nodeId) && replicaSetParticipation.containsKey(replicaSet)) {
					KafkaConsumerManager.getInstance().startConsumer(replicaSet);
				} else {
					KafkaConsumerManager.getInstance().removeConsumer(replicaSet);
				}
				replicaSetLeaders.put(replicaSet, leader);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private void create(String path) {
		try {
			curatorFramework.create().creatingParentsIfNeeded().forPath(path, null);
		} catch (Exception e) {
			if (e instanceof KeeperException) {
				KeeperException ke = (KeeperException) e;
				if (Code.NODEEXISTS == ke.code()) {
					return;
				}
			}
			throw new RuntimeException(e);
		}
	}
	
	private void readClusterNodes() {
		List<String> list = null;
		try {
			create(CLUSTER_NODES);
			list = curatorFramework.getChildren().usingWatcher(watcher).forPath(CLUSTER_NODES);
		} catch (Exception e) {
			throw new RuntimeException();
		}

		for (int i = 0; i < list.size(); i++) {
			String node = list.get(i);
			readClusterNode(node);
		}	
	}
	
	private void readClusterNode(String id) {
		String machineName = null;
		try {
			byte[] bytes = curatorFramework.getData().usingWatcher(watcher).forPath(CLUSTER_NODES + "/" + id);
			machineName = new String(bytes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		machineIdTNameMap.put(id, machineName);		
	}
	
	private void readCollections() {
		List<String> list = null;
		try {
			create(COLLECTIONS_PATH);
			curatorFramework.getData().usingWatcher(watcher).forPath(COLLECTIONS_PATH);
			list = curatorFramework.getChildren().forPath(COLLECTIONS_PATH);
		} catch (Exception e) {
			throw new RuntimeException();
		}

		for (int i = 0; i < list.size(); i++) {
			String collectionName = list.get(i);
			readCollection(collectionName);
		}			
	}
	
	private void readCollection(String collectionName) {
		String path = COLLECTIONS_PATH + "/" + collectionName;
		byte[] bytes = null;
		String storage = null;
		@SuppressWarnings("unused")
		boolean loggingEnabled = false;
		
		try {
			curatorFramework.getData().usingWatcher(watcher).forPath(path);
			bytes = curatorFramework.getData().forPath(path + "/storage");
			storage = new String(bytes);
			bytes = curatorFramework.getData().forPath(path + "/isLoggingEnabled");
			String s = new String(bytes);
			if ("true".equalsIgnoreCase(s)) {
				loggingEnabled = true;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		List<CollectionColumn> columnList = readColumns(collectionName);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (colMeta == null) {
			try {
				ClusterSchemaManager.getInstance().createCollection(collectionName, storage, columnList, false);
			} catch (InvalidCollectionNameException e) {
				e.printStackTrace();
			}
		} else {
			colMeta.addColumns(columnList);
		}
		readIndexes(collectionName);
		readShards(collectionName);
	}
	
	private void readShards(String collectionName) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/shards";
		List<String> shardList = null;
		String indexName = "";
		try {
			shardList = curatorFramework.getChildren().usingWatcher(watcher).forPath(path);
			for (int i = 0; i < shardList.size(); i++) {
				if (shardList.get(i).equals("indexName")) {
					byte[] bytes = curatorFramework.getData().forPath(path + "/indexName");
					indexName = new String(bytes);
					break;
				}
			}
			
			for (int i = 0; i < shardList.size(); i++) {
				if (!shardList.get(i).equals("indexName")) {
					readShard(collectionName, shardList.get(i), indexName);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void readShard(String collectionName, String replicaSet, String indexName) {
		if (!replicaSetParticipation.containsKey(replicaSet)) {
			return;
		}
		byte[] bytes = null;
		String path = COLLECTIONS_PATH + "/" + collectionName + "/shards/" + replicaSet;
		try {
			int idxSchemaId = SchemaMetadata.getInstance().getIndex(indexName).getSchemaId();
			bytes = curatorFramework.getData().forPath(path + "/" + "minIndexVal");
			IndexKeyType minVal = null;
			IndexKeyType maxVal = null;
			ChannelBuffer buffer = null;
			if (bytes == null || bytes.length == 0) {
				minVal = null;
			} else {
				buffer = ChannelBuffers.wrappedBuffer(bytes);
				minVal = IndexSerializer.getInstance().unmarshal(buffer, idxSchemaId);
			}
			bytes = curatorFramework.getData().forPath(path + "/" + "maxIndexVal");
			if (bytes == null || bytes.length == 0) {
				maxVal = null;
			} else {
				buffer = ChannelBuffers.wrappedBuffer(bytes);
				maxVal = IndexSerializer.getInstance().unmarshal(buffer, idxSchemaId);
			}
			int colSchemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
			Shard shard = new Shard(colSchemaId, collectionName, replicaSet);
			shard.setMaxMap(maxVal);
			shard.setMinMap(minVal);
			Set<Shard>	shards = collectionNameByShardMap.get(collectionName);
			if (shards == null) {
				shards = new HashSet<Shard>();
				collectionNameByShardMap.put(collectionName, shards);
			}
			shards.add(shard);
			SchemaMetadata.getInstance().createShard(shard, indexName, minVal, maxVal);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private List<CollectionColumn> readColumns(String collectionName) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/columns";
		List<String> list = null;
		try {
			list = curatorFramework.getChildren().forPath(path);
		} catch (Exception e) {
			throw new RuntimeException();
		}
		List<CollectionColumn> columnList = new ArrayList<CollectionColumn>();
		for (int i = 0; i < list.size(); i++) {
			String node = list.get(i);
			String columnName = getPosition(node, 0);
			CollectionColumn cc = readColumn(collectionName, columnName);
			columnList.add(cc);
		}
		return columnList;
	}
	
	private CollectionColumn readColumn(String collectionName, String columnName) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/columns/" + columnName;
		byte[] bytes = null;
		try {
			bytes = curatorFramework.getData().forPath(path + "/serializerName");
			String serializerName = new String(bytes);
			bytes = curatorFramework.getData().forPath(path + "/isNullable");
			String s = new String(bytes);
			boolean isNullable = false;
			if ("true".equalsIgnoreCase(s)) {
				isNullable = true;
			}
			boolean isQueriable = false;
			bytes = curatorFramework.getData().forPath(path + "/isQueriable");
			s = new String(bytes);
			if ("true".equalsIgnoreCase(s)) {
				isQueriable = true;
			}
			
			CollectionColumn cc = new CollectionColumn(columnName, -1, serializerName, isNullable, isQueriable);
			return cc;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createStorage(String fileName, int blockSize, boolean isDefault) {
		String path = STORAGE_PATH + "/" + fileName;
		try {
			curatorFramework.create().creatingParentsIfNeeded().forPath(path);		
			String p = path + "/blockSize";
			curatorFramework.create().forPath(p, String.valueOf(blockSize).getBytes());
			p = path + "/default";
			String defaultStr = isDefault ? "true" : "false";
			curatorFramework.create().forPath(p, String.valueOf(defaultStr).getBytes());
			curatorFramework.setData().forPath(STORAGE_PATH);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void readIndexes(String collectionName) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/indexes";
		List<String> list = null;
		try {
			curatorFramework.getData().usingWatcher(watcher).forPath(path);
			list = curatorFramework.getChildren().forPath(path);
			for(int i = 0; i < list.size(); i++) {
				readIndex(collectionName, list.get(i));				
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void createIndex(String collectionName, String indexName,
			String storageFile, List<CollectionColumn> columns,
			boolean isUnique, boolean isAsc) throws InvalidIndexException {

		String path = COLLECTIONS_PATH + "/" + collectionName + "/indexes/" + indexName;
		try {
			curatorFramework.create().creatingParentsIfNeeded().forPath(path);
			curatorFramework.create().forPath(path + "/columns");
			String uniqueVal = isUnique == true ? "true" : "false";
			String ascVal = isAsc == true ? "true" : "false";
			curatorFramework.create().forPath(path + "/isUnique", uniqueVal.getBytes());
			curatorFramework.create().forPath(path + "/isAsc", ascVal.getBytes());
			curatorFramework.create().forPath(path + "/storage", storageFile.getBytes());
			for (int i = 0; i < columns.size(); i++) {
				CollectionColumn cc = columns.get(i);
				curatorFramework.create().forPath(path + "/columns/" + cc.getColumnName());
			}
			curatorFramework.setData().forPath(COLLECTIONS_PATH + "/" + collectionName + "/indexes");
			curatorFramework.setData().forPath(COLLECTIONS_PATH + "/" + collectionName);
		} catch (Exception e) {
			if (e instanceof NodeExistsException) {
				throw new InvalidIndexException("Index already present: " + indexName);
			}
		}
	}
	
	private void readIndex(String collectionName, String indexName) {
		String path = COLLECTIONS_PATH + "/" + collectionName + "/indexes/" + indexName;
		byte[] bytes = null;
		boolean isUnique = false;
		boolean isAsc = false;
		@SuppressWarnings("unused")
		String storage = "";
		List<String> columns = new ArrayList<String>();
		try {
			bytes = curatorFramework.getData().forPath(path + "/isUnique");
			String val = new String(bytes);
			if ("true".equals(val)) {
				isUnique = true;
			}
			bytes = curatorFramework.getData().forPath(path + "/isAsc");
			val = new String(bytes);
			if ("true".equals(val)) {
				isAsc = true;
			}
			bytes = curatorFramework.getData().forPath(path + "/storage");
			storage = new String(bytes);
			columns = curatorFramework.getChildren().forPath(path + "/columns");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		IndexMetadata idxMeta = SchemaMetadata.getInstance().getIndex(indexName);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(collectionName);
		if (idxMeta == null) {
			List<CollectionColumn> list = new ArrayList<CollectionColumn>();
			for (int i = 0; i < columns.size(); i++) {
				String s = columns.get(i);
				CollectionColumn cc = colMeta.getCollectionColumn(colMeta.getColumnId(s));
				list.add(cc);
			}
			Index idx;
			try {
				List<Shard> sList = ClusterManagerFactory.getInstance().getClusterManager().getShards(collectionName);
				idx = new Index(indexName, collectionName, list, isUnique, isAsc);
//				Shard defaultIdxShard = new Shard(-1, idx.getIndexName(), idx.getCollectionName());
//				SchemaMetadata.getInstance().add(idx, defaultIdxShard);
				for (int i = 0; i < sList.size(); i++) {
					Shard shard = sList.get(i);
					SchemaMetadata.getInstance().add(idx, shard);
				}
			} catch (InvalidIndexException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidCollectionNameException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public String getMachineId() {
		// TODO Auto-generated method stub
		return nodeId;
	}	

	public static void main(String[] args) throws Exception {
		WonderDBPropertyManager.getInstance().init("server.properties");
		String zkConnectStr = WonderDBPropertyManager.getInstance().getZkConnString();
		
//		ZkClusterManager mgr = new ZkClusterManager();
//		deleteFolder(mgr.curatorFramework, args[0]);
//		mgr.createReplicaSet("RS1");
//		mgr.createReplicaSet("RS2");
//		mgr.addToReplicaSet("RS1", "0000000000");
//		mgr.addToReplicaSet("RS1", "0000000002");
		String zkConnectString = WonderDBPropertyManager.getInstance().getZkConnString();
		int connectRetryCount = WonderDBPropertyManager.getInstance().getZkConnectRetryCount();
		int sleepTime = WonderDBPropertyManager.getInstance().getZkConnectRetrySleepTime();
		int connectionTimeout = WonderDBPropertyManager.getInstance().getZkConnectionTimeout();
		int sessionTimeout = WonderDBPropertyManager.getInstance().getZkSessionTimeout();
		RetryNTimes rnt = new RetryNTimes(connectRetryCount, sleepTime);

		CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
		.connectString(zkConnectString)
		.retryPolicy(rnt)
		.connectionTimeoutMs(connectionTimeout)
		.sessionTimeoutMs(sessionTimeout)
		.build();
		curatorFramework.start();

		deleteFolder(curatorFramework, "/WonderDB");
		
//		deleteFolder(curatorFramework, "/WonderDB/storages");
//		deleteFolder(curatorFramework, "/WonderDB/collections");
//		deleteFolder(curatorFramework, "/WonderDB/clusterNodes");
////		deleteFolder(curatorFramework, "/WonderDB/storages");
//		deleteFolder(curatorFramework, "/WonderDB/curator");

//		deleteFolder(curatorFramework, "/consumers");
//		deleteFolder(curatorFramework, "/controller");
//		deleteFolder(curatorFramework, "/brokers");
//		deleteFolder(curatorFramework, "/controller_epoch");
	}
	
	public static void deleteFolder(CuratorFramework client, String path) throws Exception {
		if (path == null) {
			return;
		}
		List<String> list = client.getChildren().forPath(path);
		if (list != null && list.size() > 0) {
			for (int i = 0; i < list.size(); i++) {
				String s = list.get(i);
				String p = path + "/" + s;
				deleteFolder(client, p);
			}
			deleteFolder(client, path);
		} else {
			client.delete().forPath(path);
		}
	}
	
	public static void addToRS(String repSet, String nodeId) {		
	}
}
