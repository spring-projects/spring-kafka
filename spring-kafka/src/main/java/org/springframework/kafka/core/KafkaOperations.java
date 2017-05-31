/*
 * Copyright 2015-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

/**
 * The basic Kafka operations contract returning {@link ListenableFuture}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * If the Kafka topic is set with {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}
 * all send operations will use the user provided time if provided, else
 * {@link org.apache.kafka.clients.producer.KafkaProducer} will generate one
 *
 * If the topic is set with {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}
 * then the user provided timestamp will be ignored and instead will be the
 * Kafka broker local time when the message is appended
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Biju Kunjummen
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
	ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, V data);

	/**
	 * Send the data to the default topic with the provided key and partition.
	 * @param partition the partition.
	 * @param timestamp the timestamp of the record.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 * @since 2.0
	 */
	ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, Long timestamp, K key, V data);

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
	 * Send the data to the provided topic with the provided key and partition.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 */
	ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data);

	/**
	 * Send the data to the provided topic with the provided key and partition.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param timestamp the timestamp of the record.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 * @since 2.0
	 */
	ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, Long timestamp, K key, V data);

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
	 * Send a Flux of SenderRecords.
	 * @param flux the flux.
	 * @param <T> correlation metadata type.
	 * @return a flux of sendresults.
	 */
	<T> Flux<SenderResult<T>> send(Flux<SenderRecord<K, V, T>> flux, Class<T> t);

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
