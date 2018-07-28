/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.kafka.core.reactive;

import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;

/**
 * Reactive kafka producer operations implementation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 */
public class ReactiveKafkaProducerTemplate<K, V> implements ReactiveKafkaProducerOperations<K, V>, AutoCloseable, DisposableBean {
	private static final Log LOG = LogFactory.getLog(ReactiveKafkaProducerTemplate.class);

	private final KafkaSender<K, V> sender;
	private final RecordMessageConverter messageConverter;

	public ReactiveKafkaProducerTemplate(SenderOptions<K, V> senderOptions) {
		this(senderOptions, new MessagingMessageConverter());
	}

	public ReactiveKafkaProducerTemplate(SenderOptions<K, V> senderOptions, RecordMessageConverter messageConverter) {
		this.sender = KafkaSender.create(senderOptions);
		this.messageConverter = messageConverter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<SenderResult<Void>> send(String topic, Message<?> message) {
		ProducerRecord<?, ?> producerRecord = this.messageConverter.fromMessage(message, topic);
		return this.send((ProducerRecord<K, V>) producerRecord);
	}

	@Override
	public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records) {
		return this.sender.send(records);
	}

	@Override
	public <T> Flux<Flux<SenderResult<T>>> sendTransactionally(Publisher<? extends Publisher<? extends SenderRecord<K, V, T>>> records) {
		return this.sender.sendTransactionally(records);
	}

	@Override
	public TransactionManager transactionManager() {
		return this.sender.transactionManager();
	}

	@Override
	public KafkaOutbound<K, V> createOutbound() {
		return this.sender.createOutbound();
	}

	@Override
	public <T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> action) {
		return this.sender.doOnProducer(action);
	}

	@Override
	public void destroy() throws Exception {
		doClose();
	}

	@Override
	public void close() throws Exception {
		doClose();
	}

	private void doClose() {
		this.sender.close();
	}
}
