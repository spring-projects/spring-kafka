/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * The basic Kafka operations contract returning {@link ListenableFuture}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public interface KafkaOperations<K, V> {

	/**
	 * Send the data to the default topic with no key or partition.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> sendDefault(V data);

	/**
	 * Send the data to the default topic with the provided key and no partition.
	 * @param key the key.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> sendDefault(K key, V data);

	/**
	 * Send the data to the default topic with the provided key and partition.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> sendDefault(int partition, K key, V data);

	/**
	 * Send the data to the provided topic with no key or partition.
	 * @param topic the topic.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> send(String topic, V data);

	/**
	 * Send the data to the provided topic with the provided key and no partition.
	 * @param topic the topic.
	 * @param key the key.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> send(String topic, K key, V data);

	/**
	 * Send the data to the provided topic with the provided partition and no key.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> send(String topic, int partition, V data);

	/**
	 * Send the data to the provided topic with the provided key and partition.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> send(String topic, int partition, K key, V data);

	/**
	 * Send a message with routing information in message headers. The message payload
	 * may be converted before sending.
	 * @param message the message to send.
	 * @return a Future for the {@link SendResult}.
	 * @see org.springframework.kafka.support.KafkaHeaders#TOPIC
	 * @see org.springframework.kafka.support.KafkaHeaders#PARTITION_ID
	 * @see org.springframework.kafka.support.KafkaHeaders#MESSAGE_KEY
	 */
	ListenableFuture<SendResult<K, V>> send(Message<?> message);

	/**
	 * See {@link Producer#partitionsFor(String)}.
	 * @param topic the topic.
	 * @return the partition info.
	 * @since 1.1
	 */
	List<PartitionInfo> partitionsFor(String topic);

	/**
	 * See {@link Producer#metrics()}.
	 * @return the metrics.
	 * @since 1.1
	 */
	Map<MetricName, ? extends Metric> metrics();

	/**
	 * Execute some arbitrary operation(s) on the producer and return the result.
	 * @param callback the callback.
	 * @param <T> the result type.
	 * @return the result.
	 * @since 1.1
	 */
	<T> T execute(ProducerCallback<K, V, T> callback);

	/**
	 * Flush the producer.
	 */
	void flush();

	/**
	 * A callback for executing arbitrary operations on the {@link Producer}.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @param <T> the return type.
	 * @since 1.1
	 */
	interface ProducerCallback<K, V, T> {

		T doInKafka(Producer<K, V> producer);

	}

}
