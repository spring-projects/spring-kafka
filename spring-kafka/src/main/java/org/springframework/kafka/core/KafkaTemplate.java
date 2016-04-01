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

import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.ProducerListenerInvokingCallback;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;


/**
 * A template for executing high-level operations.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> {

	protected final Log logger = LogFactory.getLog(this.getClass()); //NOSONAR

	private final ProducerFactory<K, V> producerFactory;

	private MessageConverter messageConverter = new MessagingMessageConverter();

	private volatile Producer<K, V> producer;

	private volatile String defaultTopic;

	private volatile ProducerListener<K, V> producerListener = new LoggingProducerListener<K, V>();

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

	/**
	 * Set a {@link ProducerListener} which will be invoked when Kafka acknowledges
	 * a send operation. By default a {@link LoggingProducerListener} is configured
	 * which logs errors only.
	 * @param producerListener the listener; may be {@code null}.
	 */
	public void setProducerListener(ProducerListener<K, V> producerListener) {
		this.producerListener = producerListener;
	}

	/**
	 * Return the message converter.
	 * @return the message converter.
	 */
	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set the message converter to use.
	 * @param messageConverter the message converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public Future<RecordMetadata> send(V data) {
		return send(this.defaultTopic, data);
	}

	@Override
	public Future<RecordMetadata> send(K key, V data) {
		return send(this.defaultTopic, key, data);
	}

	@Override
	public Future<RecordMetadata> send(int partition, K key, V data) {
		return send(this.defaultTopic, partition, key, data);
	}

	@Override
	public Future<RecordMetadata> send(String topic, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata> send(String topic, K key, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata> send(String topic, int partition, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, null, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata> send(String topic, int partition, K key, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
		return doSend(producerRecord);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Future<RecordMetadata> convertAndSend(Message<?> message) {
		ProducerRecord<?, ?> producerRecord = this.messageConverter.fromMessage(message, this.defaultTopic);
		return doSend((ProducerRecord<K, V>) producerRecord);
	}

	@Override
	public RecordMetadata syncSend(V data) throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncSend(K key, V data) throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncSend(int partition, K key, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(partition, key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncSend(String topic, V data) throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(topic, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncSend(String topic, K key, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(topic, key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncSend(String topic, int partition, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(topic, partition, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncSend(String topic, int partition, K key, V data)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = send(topic, partition, key, data);
		flush();
		return future.get();
	}

	@Override
	public RecordMetadata syncConvertAndSend(Message<?> message)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = convertAndSend(message);
		flush();
		return future.get();
	}

	@Override
	public void flush() {
		this.producer.flush();
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
		Future<RecordMetadata> future;
		if (this.producerListener == null) {
			future = this.producer.send(producerRecord);
		}
		else {
			future = this.producer.send(producerRecord,
					new ProducerListenerInvokingCallback<>(producerRecord.topic(), producerRecord.partition(),
							producerRecord.key(), producerRecord.value(), this.producerListener));
		}
		if (this.logger.isTraceEnabled()) {
			this.logger.trace("Sent: " + producerRecord);
		}
		return future;
	}

}
