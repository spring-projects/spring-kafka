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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Reactive kafka consumer operations implementation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 */
public class ReactiveKafkaConsumerTemplate<K, V> implements ReactiveKafkaConsumerOperations<K, V> {
	private final KafkaReceiver<K, V> kafkaReceiver;

	public ReactiveKafkaConsumerTemplate(ReceiverOptions<K, V> receiverOptions) {
		this.kafkaReceiver = KafkaReceiver.create(receiverOptions);
	}

	@Override
	public Flux<ReceiverRecord<K, V>> receive() {
		return this.kafkaReceiver.receive();
	}

	@Override
	public Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck() {
		return this.kafkaReceiver.receiveAutoAck();
	}

	@Override
	public Flux<ConsumerRecord<K, V>> receiveAtmostOnce() {
		return this.kafkaReceiver.receiveAtmostOnce();
	}

	@Override
	public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
		return this.kafkaReceiver.receiveExactlyOnce(transactionManager);
	}

	@Override
	public <T> Mono<T> doOnConsumer(Function<Consumer<K, V>, ? extends T> function) {
		return this.kafkaReceiver.doOnConsumer(function);
	}

	public Flux<TopicPartition> assignment() {
		Mono<Set<TopicPartition>> partitions = doOnConsumer(Consumer::assignment);
		return partitions.flatMapIterable(Function.identity());
	}

	public Mono<Set<String>> subscription() {
		return doOnConsumer(Consumer::subscription);
	}

	public Mono<Void> seek(TopicPartition partition, long offset) {
		return doOnConsumer(consumer -> {
			consumer.seek(partition, offset);
			return null;
		});
	}

	public Mono<Void> seekToBeginning(Collection<TopicPartition> partitions) {
		return doOnConsumer(consumer -> {
			consumer.seekToBeginning(partitions);
			return null;
		});
	}

	public Mono<Void> seekToEnd(Collection<TopicPartition> partitions) {
		return doOnConsumer(consumer -> {
			consumer.seekToEnd(partitions);
			return null;
		});
	}

	public Mono<Long> position(TopicPartition partition) {
		return doOnConsumer(consumer -> consumer.position(partition));
	}

	public Mono<OffsetAndMetadata> committed(TopicPartition partition) {
		return doOnConsumer(consumer -> consumer.committed(partition));
	}

	public Mono<List<PartitionInfo>> partitionsFromConsumerFor(String topic) {
		return doOnConsumer(c -> c.partitionsFor(topic));
	}

	public Mono<? extends Map<MetricName, ? extends Metric>> metricsFromConsumer() {
		return doOnConsumer(Consumer::metrics);
	}

	public Mono<Map<String, List<PartitionInfo>>> listTopics() {
		return doOnConsumer(Consumer::listTopics);
	}

	public Mono<Set<TopicPartition>> paused() {
		return doOnConsumer(Consumer::paused);
	}

	public Mono<Void> pause(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> {
			c.pause(partitions);
			return null;
		});
	}

	public Mono<Void> resume(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> {
			c.resume(partitions);
			return null;
		});
	}

	public Mono<Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		return doOnConsumer(c -> c.offsetsForTimes(timestampsToSearch));
	}

	public Mono<Map<TopicPartition, Long>> beginningOffsets(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> c.beginningOffsets(partitions));
	}

	public Mono<Map<TopicPartition, Long>> endOffsets(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> c.endOffsets(partitions));
	}
}
