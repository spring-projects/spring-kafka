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

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
	public <T> Flux<Flux<SenderResult<T>>> sendTransactionally(Publisher<? extends Publisher<? extends SenderRecord<K, V, T>>> records) {
		return this.sender.sendTransactionally(records);
	}

	public <T> Mono<SenderResult<T>> sendTransactionally(SenderRecord<K, V, T> record) {
		Flux<Flux<SenderResult<T>>> sendTransactionally = sendTransactionally(Mono.just(Mono.just(record)));
		return sendTransactionally
				.concatMap(Flux::next)
				.last();
	}

	public Mono<SenderResult<Void>> send(String topic, V value) {
		return send(new ProducerRecord<>(topic, value));
	}

	public Mono<SenderResult<Void>> send(String topic, K key, V value) {
		return send(new ProducerRecord<>(topic, key, value));
	}

	public Mono<SenderResult<Void>> send(String topic, int partition, K key, V value) {
		return send(new ProducerRecord<>(topic, partition, key, value));
	}

	public Mono<SenderResult<Void>> send(String topic, int partition, long timestamp, K key, V value) {
		return send(new ProducerRecord<>(topic, partition, timestamp, key, value));
	}

	public Mono<SenderResult<Void>> send(String topic, Message<?> message) {
		@SuppressWarnings("unchecked")
		ProducerRecord<K, V> producerRecord = (ProducerRecord<K, V>) this.messageConverter.fromMessage(message, topic);
		return send(producerRecord);
	}

	public Mono<SenderResult<Void>> send(ProducerRecord<K, V> record) {
		return send(SenderRecord.create(record, null));
	}

	public <T> Mono<SenderResult<T>> send(SenderRecord<K, V, T> record) {
		return send(Mono.just(record)).single();
	}

	@Override
	public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records) {
		return this.sender.send(records);
	}

	public Mono<Void> flush() {
		return doOnProducer(producer -> {
			producer.flush();
			return null;
		});
	}

	public Mono<List<PartitionInfo>> partitionsFromProducerFor(String topic) {
		return doOnProducer(producer -> producer.partitionsFor(topic));
	}

	public Mono<? extends Map<MetricName, ? extends Metric>> metricsFromProducer() {
		return doOnProducer(Producer::metrics);
	}

	@Override
	public <T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> action) {
		return this.sender.doOnProducer(action);
	}

	public Mono<Void> sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerId) {
		return transactionManager().sendOffsets(offsets, consumerId);
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
