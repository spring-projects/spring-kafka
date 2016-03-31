/*
 * Copyright 2015-2016 the original author or authors.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;

import org.springframework.messaging.Message;

/**
 * The basic Kafka operations contract.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public interface KafkaOperations<K, V> {

	// Async methods

	/**
	 * Send the data to the default topic with no key or partition.
	 * @param data The data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(V data);

	/**
	 * Send the data to the default topic with the provided key and no partition.
	 * @param key the key.
	 * @param data The data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(K key, V data);

	/**
	 * Send the data to the default topic with the provided key and partition.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(int partition, K key, V data);

	/**
	 * Send the data to the provided topic with no key or partition.
	 * @param topic the topic.
	 * @param data The data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(String topic, V data);

	/**
	 * Send the data to the provided topic with the provided key and no partition.
	 * @param topic the topic.
	 * @param key the key.
	 * @param data The data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(String topic, K key, V data);

	/**
	 * Send the data to the provided topic with the provided partition and no key.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param data The data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(String topic, int partition, V data);

	/**
	 * Send the data to the provided topic with the provided key and partition.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	Future<RecordMetadata> send(String topic, int partition, K key, V data);

	/**
	 * Send a message with routing information in message headers. The message payload
	 * may be converted before sending.
	 * @param message the message to send.
	 * @return a Future for the {@link RecordMetadata}.
	 * @see org.springframework.kafka.support.KafkaHeaders#TOPIC
	 * @see org.springframework.kafka.support.KafkaHeaders#PARTITION_ID
	 * @see org.springframework.kafka.support.KafkaHeaders#MESSAGE_KEY
	 */
	Future<RecordMetadata> convertAndSend(Message<?> message);


	// Sync methods

	/**
	 * Send the data to the default topic with no key or partition;
	 * wait for result.
	 * @param data The data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(V data) throws InterruptedException, ExecutionException;

	/**
	 * Send the data to the default topic with the provided key and no partition;
	 * wait for result.
	 * @param key the key.
	 * @param data The data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(K key, V data) throws InterruptedException, ExecutionException;

	/**
	 * Send the data to the default topic with the provided key and partition.
	 * wait for result.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(int partition, K key, V data) throws InterruptedException, ExecutionException;

	/**
	 * Send the data to the provided topic with no key or partition;
	 * wait for result.
	 * @param topic the topic.
	 * @param data The data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(String topic, V data) throws InterruptedException, ExecutionException;

	/**
	 * Send the data to the provided topic with the provided key and no partition;
	 * wait for result.
	 * @param topic the topic.
	 * @param key the key.
	 * @param data The data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(String topic, K key, V data) throws InterruptedException, ExecutionException;

	/**
	 * Send the data to the provided topic with the provided partition and no key;
	 * wait for result.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param data The data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(String topic, int partition, V data) throws InterruptedException, ExecutionException;

	/**
	 * Send the data to the provided topic with the provided key and partition;
	 * wait for result.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 */
	RecordMetadata syncSend(String topic, int partition, K key, V data)
		throws InterruptedException, ExecutionException;

	/**
	 * Send a message with routing information in message headers. The message payload
	 * may be converted before sending.
	 * @param message the message to send.
	 * @return a Future for the {@link RecordMetadata}.
	 * @throws ExecutionException execution exception while awaiting result.
	 * @throws InterruptedException thread interrupted while awaiting result.
	 * @see org.springframework.kafka.support.KafkaHeaders#TOPIC
	 * @see org.springframework.kafka.support.KafkaHeaders#PARTITION_ID
	 * @see org.springframework.kafka.support.KafkaHeaders#MESSAGE_KEY
	 */
	RecordMetadata syncConvertAndSend(Message<?> message)
		throws InterruptedException, ExecutionException;

	/**
	 * Flush the producer.
	 */
	void flush();

}
