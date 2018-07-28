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

import org.springframework.messaging.Message;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;


/**
 * Interface for executing reactive kafka operations on producer side.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 */
public interface ReactiveKafkaProducerOperations<K, V> {

	default Mono<SenderResult<Void>> send(String topic, V value) {
		return send(new ProducerRecord<>(topic, value));
	}

	default Mono<SenderResult<Void>> send(String topic, K key, V value) {
		return send(new ProducerRecord<>(topic, key, value));
	}

	default Mono<SenderResult<Void>> send(String topic, int partition, K key, V value) {
		return send(new ProducerRecord<>(topic, partition, key, value));
	}

	default Mono<SenderResult<Void>> send(String topic, int partition, long timestamp, K key, V value) {
		return send(new ProducerRecord<>(topic, partition, timestamp, key, value));
	}

	default Mono<SenderResult<Void>> send(ProducerRecord<K, V> record) {
		return send(SenderRecord.create(record, null));
	}

	default <T> Mono<SenderResult<T>> send(SenderRecord<K, V, T> record) {
		return send(Mono.just(record)).single();
	}

	Mono<SenderResult<Void>> send(String topic, Message<?> message);

	<T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records);

	default <T> Mono<SenderResult<T>> sendTransactionally(SenderRecord<K, V, T> record) {
		Flux<Flux<SenderResult<T>>> sendTransactionally = sendTransactionally(Mono.just(Mono.just(record)));
		return sendTransactionally
				.concatMap(Flux::next)
				.last();
	}

	<T> Flux<Flux<SenderResult<T>>> sendTransactionally(Publisher<? extends Publisher<? extends SenderRecord<K, V, T>>> records);

	TransactionManager transactionManager();

	KafkaOutbound<K, V> createOutbound();

	<T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> action);

	default Mono<Void> flush() {
		return doOnProducer(producer -> {
			producer.flush();
			return null;
		});
	}

	default Mono<List<PartitionInfo>> partitionsFromProducerFor(String topic) {
		return doOnProducer(producer -> producer.partitionsFor(topic));
	}

	default Mono<? extends Map<MetricName, ? extends Metric>> metricsFromProducer() {
		return doOnProducer(Producer::metrics);
	}

	default Mono<Void> sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerId) {
		return transactionManager().sendOffsets(offsets, consumerId);
	}
}
