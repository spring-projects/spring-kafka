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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * A template for executing high-level operations.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final ProducerFactory<K, V> producerFactory;

	private volatile Producer<K, V> producer;

	private volatile String defaultTopic;

	/**
	 * Create an instance using the supplied producer factory.
	 * @param producerFactory the producer factory.
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory) {
		this.producerFactory = producerFactory;
	}

	/**
	 * The default topic for send methods where a topic is not
	 * providing.
	 * @return the topic.
	 */
	public String getDefaultTopic() {
		return this.defaultTopic;
	}

	/**
	 * Set the default topic for send methods where a topic is not
	 * providing.
	 * @param defaultTopic the topic.
	 */
	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(V data) {
		return convertAndSend(this.defaultTopic, data);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(K key, V data) {
		return convertAndSend(this.defaultTopic, key, data);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(int partition, K key, V data) {
		return convertAndSend(this.defaultTopic, partition, key, data);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(String topic, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(String topic, K key, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(String topic, int partition, K key, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
		return doSend(producerRecord);
	}


	@Override
	public RecordMetadata syncConvertAndSend(V data) throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncConvertAndSend(K key, V data) throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncConvertAndSend(int partition, K key, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(partition, key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncConvertAndSend(String topic, V data) throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(topic, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncConvertAndSend(String topic, K key, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(topic, key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncConvertAndSend(String topic, int partition, K key, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(topic, partition, key, data);
		flush();
		return future.get();
	}

	/**
	 * Send the producer record.
	 * @param producerRecord the producer record.
	 * @return a Future for the {@link RecordMetadata}.
	 */
	protected Future<RecordMetadata> doSend(ProducerRecord<K, V> producerRecord) {
		if (this.producer == null) {
			synchronized (this) {
				if (this.producer == null) {
					this.producer = this.producerFactory.createProducer();
				}
			}
		}
		if (this.logger.isTraceEnabled()) {
			this.logger.trace("Sending: " + producerRecord);
		}
		Future<RecordMetadata> future = this.producer.send(producerRecord);
		if (this.logger.isTraceEnabled()) {
			this.logger.trace("Sent: " + producerRecord);
		}
		return future;
	}

	@Override
	public void flush() {
		this.producer.flush();
	}

}
