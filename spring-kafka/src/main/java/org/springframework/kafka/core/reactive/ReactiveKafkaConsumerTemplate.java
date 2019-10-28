/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.core.reactive;

import java.util.Arrays;
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

import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Reactive kafka consumer operations implementation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 *
 * @since 2.3.0
 */
public class ReactiveKafkaConsumerTemplate<K, V> {

	private final KafkaReceiver<K, V> kafkaReceiver;

	public ReactiveKafkaConsumerTemplate(ReceiverOptions<K, V> receiverOptions) {
		Assert.notNull(receiverOptions, "Receiver options can not be null");
		this.kafkaReceiver = KafkaReceiver.create(receiverOptions);
	}

	public Flux<ReceiverRecord<K, V>> receive() {
		return this.kafkaReceiver.receive();
	}

	public Flux<ConsumerRecord<K, V>> receiveAutoAck() {
		return this.kafkaReceiver.receiveAutoAck().flatMap(Function.identity());
	}

	public Flux<ConsumerRecord<K, V>> receiveAtMostOnce() {
		return this.kafkaReceiver.receiveAtmostOnce();
	}

	/**
	 * Returns a {@link Flux} of consumer record batches that may be used for exactly once
	 * delivery semantics. A new transaction is started for each inner Flux and it is the
	 * responsibility of the consuming application to commit or abort the transaction
	 * using {@link TransactionManager#commit()} or {@link TransactionManager#abort()}
	 * after processing the Flux. The next batch of consumer records will be delivered only
	 * after the previous flux terminates. Offsets of records dispatched on each inner Flux
	 * are committed using the provided <code>transactionManager</code> within the transaction
	 * started for that Flux.
	 * <p> Example usage:
	 * <pre>
	 * {@code
	 * KafkaSender<Integer, Person> sender = sender(senderOptions());
	 * ReceiverOptions<Integer, Person> receiverOptions = receiverOptions(Collections.singleton(sourceTopic));
	 * KafkaReceiver<Integer, Person> receiver = KafkaReceiver.create(receiverOptions);
	 * receiver.receiveExactlyOnce(sender.transactionManager())
	 * 	 .concatMap(f -> sendAndCommit(f))
	 *	 .onErrorResume(e -> sender.transactionManager().abort().then(Mono.error(e)))
	 *	 .doOnCancel(() -> close());
	 *
	 * Flux<SenderResult<Integer>> sendAndCommit(Flux<ConsumerRecord<Integer, Person>> flux) {
	 * 	return sender.send(flux.map(r -> SenderRecord.<Integer, Person, Integer>create(transform(r.value()), r.key())))
	 *			.concatWith(sender.transactionManager().commit());
	 * }
	 * }
	 * </pre>
	 * @param transactionManager Transaction manager used to begin new transaction for each
	 *        inner Flux and commit offsets within that transaction
	 * @return Flux of consumer record batches processed within a transaction
	 */
	public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
		return this.kafkaReceiver.receiveExactlyOnce(transactionManager);
	}

	public <T> Mono<T> doOnConsumer(Function<Consumer<K, V>, ? extends T> function) {
		return this.kafkaReceiver.doOnConsumer(function);
	}

	public Flux<TopicPartition> assignment() {
		Mono<Set<TopicPartition>> partitions = doOnConsumer(Consumer::assignment);
		return partitions.flatMapIterable(Function.identity());
	}

	public Flux<String> subscription() {
		Mono<Set<String>> subscriptions = doOnConsumer(Consumer::subscription);
		return subscriptions.flatMapIterable(Function.identity());
	}

	public Mono<Void> seek(TopicPartition partition, long offset) {
		return doOnConsumer(consumer -> {
			consumer.seek(partition, offset);
			return null;
		});
	}

	public Mono<Void> seekToBeginning(TopicPartition... partitions) {
		return doOnConsumer(consumer -> {
			consumer.seekToBeginning(Arrays.asList(partitions));
			return null;
		});
	}

	public Mono<Void> seekToEnd(TopicPartition... partitions) {
		return doOnConsumer(consumer -> {
			consumer.seekToEnd(Arrays.asList(partitions));
			return null;
		});
	}

	public Mono<Long> position(TopicPartition partition) {
		return doOnConsumer(consumer -> consumer.position(partition));
	}

	/**
	 * Get the committed {@link OffsetAndMetadata} for the partition.
	 * @param partition the partition.
	 * @return the {@link OffsetAndMetadata}.
	 * @deprecated in favor of {@link #committed(Set)}.
	 */
	@Deprecated
	@SuppressWarnings("deprecation")
	public Mono<OffsetAndMetadata> committed(TopicPartition partition) {
		return doOnConsumer(consumer -> consumer.committed(partition));
	}

	public Mono<Map<TopicPartition, OffsetAndMetadata>> committed(Set<TopicPartition> partitions) {
		return doOnConsumer(consumer -> consumer.committed(partitions));
	}

	public Flux<PartitionInfo> partitionsFromConsumerFor(String topic) {
		Mono<List<PartitionInfo>> partitions = doOnConsumer(c -> c.partitionsFor(topic));
		return partitions.flatMapIterable(Function.identity());
	}

	public Flux<TopicPartition> paused() {
		Mono<Set<TopicPartition>> paused = doOnConsumer(Consumer::paused);
		return paused.flatMapIterable(Function.identity());
	}

	public Mono<Void> pause(TopicPartition... partitions) {
		return doOnConsumer(c -> {
			c.pause(Arrays.asList(partitions));
			return null;
		});
	}

	public Mono<Void> resume(TopicPartition... partitions) {
		return doOnConsumer(c -> {
			c.resume(Arrays.asList(partitions));
			return null;
		});
	}

	public Flux<Tuple2<MetricName, ? extends Metric>> metricsFromConsumer() {
		return doOnConsumer(Consumer::metrics)
				.flatMapIterable(Map::entrySet)
				.map(m -> Tuples.of(m.getKey(), m.getValue()));
	}

	public Flux<Tuple2<String, List<PartitionInfo>>> listTopics() {
		return doOnConsumer(Consumer::listTopics)
				.flatMapIterable(Map::entrySet)
				.map(topicAndPartition -> Tuples.of(topicAndPartition.getKey(), topicAndPartition.getValue()));
	}

	public Flux<Tuple2<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(
			Map<TopicPartition, Long> timestampsToSearch) {

		return doOnConsumer(c -> c.offsetsForTimes(timestampsToSearch))
				.flatMapIterable(Map::entrySet)
				.map(partitionAndOffset -> Tuples.of(partitionAndOffset.getKey(), partitionAndOffset.getValue()));
	}

	public Flux<Tuple2<TopicPartition, Long>> beginningOffsets(TopicPartition... partitions) {
		return doOnConsumer(c -> c.beginningOffsets(Arrays.asList(partitions)))
				.flatMapIterable(Map::entrySet)
				.map(partitionsOffsets -> Tuples.of(partitionsOffsets.getKey(), partitionsOffsets.getValue()));
	}

	public Flux<Tuple2<TopicPartition, Long>> endOffsets(TopicPartition... partitions) {
		return doOnConsumer(c -> c.endOffsets(Arrays.asList(partitions)))
				.flatMapIterable(Map::entrySet)
				.map(partitionsOffsets -> Tuples.of(partitionsOffsets.getKey(), partitionsOffsets.getValue()));
	}

}
