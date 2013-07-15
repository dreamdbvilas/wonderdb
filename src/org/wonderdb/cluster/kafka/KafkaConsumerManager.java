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
package org.wonderdb.cluster.kafka;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.wonderdb.block.record.manager.ObjectLocker;
import org.wonderdb.block.record.manager.TableRecordManager;
import org.wonderdb.cluster.Shard;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.thread.ThreadPoolExecutorWrapper;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;

public class KafkaConsumerManager {
	ConcurrentMap<String, ConsumerConnector> map = new ConcurrentHashMap<String, ConsumerConnector>();
	
	private static KafkaConsumerManager instance = new KafkaConsumerManager();
	
	ThreadPoolExecutorWrapper collectionConsumer = new ThreadPoolExecutorWrapper(WonderDBPropertyManager.getInstance().getWriterThreadPoolCoreSize(),
			WonderDBPropertyManager.getInstance().getWriterThreadPoolMaxSize(), 5, 
			WonderDBPropertyManager.getInstance().getWriterThreadPoolQueueSize());

	ThreadPoolExecutorWrapper statmentExecutor = new ThreadPoolExecutorWrapper(WonderDBPropertyManager.getInstance().getWriterThreadPoolCoreSize(),
			WonderDBPropertyManager.getInstance().getWriterThreadPoolMaxSize(), 5, 
			WonderDBPropertyManager.getInstance().getWriterThreadPoolQueueSize());

	public static KafkaConsumerManager getInstance() {
		return instance;
	}	
	
	public void shutdown() {
		Iterator<ConsumerConnector> iter = map.values().iterator();
		while (iter.hasNext()) {
			ConsumerConnector cc = iter.next();
			cc.shutdown();
		}
		collectionConsumer.shutdown();
		statmentExecutor.shutdown();
	}

	public void startConsumer(String replicaSet) {
		if (WonderDBPropertyManager.getInstance().isReplicationEnabled() && !map.containsKey(replicaSet)) {
			ReplicaSetMessageConsumerTask task = new ReplicaSetMessageConsumerTask(replicaSet);
			collectionConsumer.asynchrounousExecute(task);
		}
	}
	
	public void removeConsumer(String replicaSet) {
		ConsumerConnector consumer = map.remove(replicaSet);
		if (consumer != null) {
			consumer.shutdown();
		}
	}
	
	
	private class ReplicaSetMessageConsumerTask implements Callable<Integer> {
		String replicaSet = null;
		
		public ReplicaSetMessageConsumerTask(String replicaSet) {
			this.replicaSet = replicaSet;
		}
		
		@Override
		public Integer call() {
			ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
	                createConsumerConfig());
			map.put(replicaSet, consumer);
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(replicaSet, 1);
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(replicaSet);
	        
	        for (KafkaStream<byte[], byte[]> stream : streams) {
	        	ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        	
	            while (it.hasNext()) {
	            	MessageAndMetadata<byte[], byte[]> mam = it.next();
	            	byte[] payload = mam.message();
	            	KafkaSerializer ser = new KafkaSerializer(null);
	            	KafkaPayload p = ser.fromBytes(payload);
	            	StatementExecutorTask executor = new StatementExecutorTask(replicaSet, p);
	            	statmentExecutor.asynchrounousExecute(executor);
	            }
	            System.out.println("Shutting down Thread: ");
	        }
	        
	        return 0;
		}		
		
		private ConsumerConfig createConsumerConfig() {
	        Properties props = new Properties();
	        props.put("zookeeper.connect", "localhost:2181");
	        props.put("zookeeper.connect.timeout", "30000");
	        props.put("zookeeper.connection.timeout", "30000");
	        props.put("group.id", replicaSet);
	        props.put("zookeeper.session.timeout.ms", "30000");
	        props.put("zookeeper.sync.time.ms", "200");
	        props.put("auto.commit.interval.ms", "1000");
	        return new ConsumerConfig(props);
	    }

	}
	
	private class StatementExecutorTask implements Callable<Integer> {
		KafkaPayload payload = null;
		String replicaSet = null;
		
		StatementExecutorTask(String replicaSet, KafkaPayload payload) {
			this.payload = payload;
			this.replicaSet = replicaSet;
		}
		
		@Override
		public Integer call() throws Exception {

			String objectId = payload.objectId;
			String collectionName = payload.collectionName;
			Map<ColumnType, DBType> map = payload.map;
			int schemaId = SchemaMetadata.getInstance().getCollectionMetadata(collectionName).getSchemaId();
			Shard shard = new Shard(schemaId, collectionName, replicaSet);
			
			ObjectLocker.getInstance().acquireLock(objectId);
			try {
				if ("insert".equals(payload.queryType)) {
					return TableRecordManager.getInstance().addTableRecord(collectionName, map, shard);
				}
				
				if ("update".equals(payload.queryType)) {
					return TableRecordManager.getInstance().updateTableRecord(collectionName, objectId, map, shard);
				}
				
				if ("delete".equals(payload.queryType)) {
					return TableRecordManager.getInstance().delete(collectionName, objectId, shard);
				}
			} finally {
				ObjectLocker.getInstance().releaseLock(objectId);
			}
			return null;
		}
		
	}
}
