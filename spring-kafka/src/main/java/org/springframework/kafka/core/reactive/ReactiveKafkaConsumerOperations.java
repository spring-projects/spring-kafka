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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;


/**
 * Interface for executing reactive kafka operations on consumer side.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 */
public interface ReactiveKafkaConsumerOperations<K, V> {

	Flux<ReceiverRecord<K, V>> receive();

	Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck();

	Flux<ConsumerRecord<K, V>> receiveAtmostOnce();

	Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager);

	<T> Mono<T> doOnConsumer(Function<Consumer<K, V>, ? extends T> function);

	default Mono<Set<TopicPartition>> assignment() {
		return doOnConsumer(Consumer::assignment);
	}

	default Mono<Set<String>> subscription() {
		return doOnConsumer(Consumer::subscription);
	}

	default Mono<Void> seek(TopicPartition partition, long offset) {
		return doOnConsumer(consumer -> {
			consumer.seek(partition, offset);
			return null;
		});
	}

	default Mono<Void> seekToBeginning(Collection<TopicPartition> partitions) {
		return doOnConsumer(consumer -> {
			consumer.seekToBeginning(partitions);
			return null;
		});
	}

	default Mono<Void> seekToEnd(Collection<TopicPartition> partitions) {
		return doOnConsumer(consumer -> {
			consumer.seekToEnd(partitions);
			return null;
		});
	}

	default Mono<Long> position(TopicPartition partition) {
		return doOnConsumer(consumer -> consumer.position(partition));
	}

	default Mono<OffsetAndMetadata> commited(TopicPartition partition) {
		return doOnConsumer(consumer -> consumer.committed(partition));
	}

	default Mono<List<PartitionInfo>> partitionsFromConsumerFor(String topic) {
		return doOnConsumer(c -> c.partitionsFor(topic));
	}

	default Mono<? extends Map<MetricName, ? extends Metric>> metricsFromConsumer() {
		return doOnConsumer(Consumer::metrics);
	}

	default Mono<Map<String, List<PartitionInfo>>> listTopics() {
		return doOnConsumer(Consumer::listTopics);
	}

	default Mono<Set<TopicPartition>> paused() {
		return doOnConsumer(Consumer::paused);
	}

	default Mono<Void> pause(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> {
			c.pause(partitions);
			return null;
		});
	}

	default Mono<Void> resume(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> {
			c.resume(partitions);
			return null;
		});
	}

	default Mono<Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		return doOnConsumer(c -> c.offsetsForTimes(timestampsToSearch));
	}

	default Mono<Map<TopicPartition, Long>> beginningOffsets(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> c.beginningOffsets(partitions));
	}

	default Mono<Map<TopicPartition, Long>> endOffsets(Collection<TopicPartition> partitions) {
		return doOnConsumer(c -> c.endOffsets(partitions));
	}
}

