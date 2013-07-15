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

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wonderdb.server.WonderDBPropertyManager;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaProducerManager {
	ConcurrentMap<String, Producer<String, KafkaPayload>> producerMap = new ConcurrentHashMap<String, Producer<String,KafkaPayload>>();
	
	private static KafkaProducerManager instance = new KafkaProducerManager();
	private KafkaProducerManager() {
	}
	
	public static KafkaProducerManager getInstance() {
		return instance;
	}
	
	public Producer<String, KafkaPayload> getProducer(String replicaSetName) {
		return producerMap.get(replicaSetName);
	}
	
	public void shutdown() {
		Iterator<Producer<String, KafkaPayload>> iter = producerMap.values().iterator();
		while (iter.hasNext()) {
			iter.next().close();
		}
	}
	
	public void createProducer(String replicaSetName) {
		String brokerList = WonderDBPropertyManager.getInstance().getKafkaBrokerList();
		if (brokerList == null) {
			return;
		}
		Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("serializer.class", "org.wonderdb.cluster.kafka.KafkaSerializer");
        props.put("request.required.acks", "1");
        props.put("auto.create.topics.enable", "true");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, KafkaPayload> producer = new Producer<String, KafkaPayload>(config);
        producerMap.put(replicaSetName, producer);
	}
	
	public void removeProducer(String replicaSet) {
		Producer<String, KafkaPayload> producer = producerMap.remove(replicaSet);
		if (producer != null) {
			producer.close();
		}
		
	}
}
