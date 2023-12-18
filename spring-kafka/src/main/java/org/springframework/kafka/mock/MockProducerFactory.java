/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.kafka.mock;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Support the use of {@link MockProducer} in tests.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Pawel Szymczyk
 * @since 3.0.7
 */
public class MockProducerFactory<K, V> implements ProducerFactory<K, V> {

	private final BiFunction<Boolean, String, MockProducer<K, V>> producerProvider;

	@Nullable
	private final String defaultTxId;

	private final boolean transactional;

	/**
	 * Create an instance that does not support transactional producers.
	 *
	 * @param producerProvider a {@link Supplier} for a {@link MockProducer}.
	 */
	public MockProducerFactory(Supplier<MockProducer<K, V>> producerProvider) {
		this.producerProvider = (tx, id) -> producerProvider.get();
		this.defaultTxId = null;
		this.transactional = false;
	}

	/**
	 * Create an instance that supports transactions, with the supplied producer provider {@link BiFunction}. The
	 * function has two parameters, a boolean indicating whether a transactional producer
	 * is being requested and, if true, the transaction id prefix for that producer.
	 *
	 * @param producerProvider the provider function.
	 * @param defaultTxId the default transactional id.
	 */
	public MockProducerFactory(BiFunction<Boolean, String, MockProducer<K, V>> producerProvider,
							   @Nullable String defaultTxId) {

		this.producerProvider = producerProvider;
		this.defaultTxId = defaultTxId;
		this.transactional = true;
	}

	@Override
	public boolean transactionCapable() {
		return this.transactional;
	}

	@Override
	public Producer<K, V> createProducer() {
		return createProducer(this.defaultTxId);
	}

	@Override
	public Producer<K, V> createProducer(@Nullable String txIdPrefix) {
		return txIdPrefix == null && this.defaultTxId == null
				? new CloseSafeMockProducer<>(this.producerProvider.apply(false, null))
				: new CloseSafeMockProducer<>(this.producerProvider.apply(true, txIdPrefix == null ? this.defaultTxId : txIdPrefix));
	}

	@Override
	public Producer<K, V> createNonTransactionalProducer() {
		return this.producerProvider.apply(false, null);
	}

	/**
	 * A wrapper class for the delegate, inspired by {@link DefaultKafkaProducerFactory.CloseSafeProducer}.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 * @author Pawel Szymczyk
	 */
	static class CloseSafeMockProducer<K, V> implements Producer<K, V> {

		private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(CloseSafeMockProducer.class));

		private final MockProducer<K, V> delegate;

		CloseSafeMockProducer(MockProducer<K, V> delegate) {
			this.delegate = delegate;
		}

		@Override
		public void initTransactions() {
			this.delegate.initTransactions();
		}

		@Override
		public void beginTransaction() throws ProducerFencedException {
			this.delegate.beginTransaction();
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
			this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
			this.delegate.sendOffsetsToTransaction(offsets, groupMetadata);
		}

		@Override
		public void commitTransaction() throws ProducerFencedException {
			this.delegate.commitTransaction();
		}

		@Override
		public void abortTransaction() throws ProducerFencedException {
			this.delegate.abortTransaction();
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			return this.delegate.send(record);
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			return this.delegate.send(record, callback);
		}

		@Override
		public void flush() {
			this.delegate.flush();
		}

		@Override
		public List<PartitionInfo> partitionsFor(String topic) {
			return this.delegate.partitionsFor(topic);
		}

		@Override
		public Map<MetricName, ? extends Metric> metrics() {
			return this.delegate.metrics();
		}

		@Override
		public void close() {
			close(null);
		}

		@Override
		public void close(Duration timeout) {
			LOGGER.debug(() -> "The closing of delegate producer " + delegate + "has been skipped.");
		}
	}
}
