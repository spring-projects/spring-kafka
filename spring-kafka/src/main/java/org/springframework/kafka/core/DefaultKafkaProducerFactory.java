/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.util.Assert;

/**
 * The {@link ProducerFactory} implementation for the {@code singleton} shared {@link Producer}
 * instance.
 * <p>
 * This implementation will produce a new {@link Producer} instance (if transactions are not enabled).
 * for provided {@link Map} {@code configs} and optional {@link Serializer} {@code keySerializer},
 * {@code valueSerializer} implementations on each {@link #createProducer()}
 * invocation.
 * <p>
 * The {@link Producer} instance is freed from the external {@link Producer#close()} invocation
 * with the internal wrapper. The real {@link Producer#close()} is called on the target
 * {@link Producer} during the {@link Lifecycle#stop()} or {@link DisposableBean#destroy()}.
 * <p>
 * Setting {@link #setTransactionIdPrefix(String)} enables transactions; in which case, a cache
 * of producers is maintained; closing the producer returns it to the cache.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 */
public class DefaultKafkaProducerFactory<K, V> implements ProducerFactory<K, V>, Lifecycle, DisposableBean {

	private static final int DEFAULT_PHYSICAL_CLOSE_TIMEOUT = 30;

	private static final Log logger = LogFactory.getLog(DefaultKafkaProducerFactory.class);

	private final Map<String, Object> configs;

	private final AtomicInteger transactionIdSuffix = new AtomicInteger();

	private final BlockingQueue<CloseSafeProducer<K, V>> cache = new LinkedBlockingQueue<>();

	private final Map<String, CloseSafeProducer<K, V>> consumerProducers = new HashMap<>();

	private volatile CloseSafeProducer<K, V> producer;

	private Serializer<K> keySerializer;

	private Serializer<V> valueSerializer;

	private int physicalCloseTimeout = DEFAULT_PHYSICAL_CLOSE_TIMEOUT;

	private String transactionIdPrefix;

	private volatile boolean running;

	private boolean producerPerConsumerPartition = true;

	/**
	 * Construct a factory with the provided configuration.
	 * @param configs the configuration.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	public DefaultKafkaProducerFactory(Map<String, Object> configs, Serializer<K> keySerializer,
			Serializer<V> valueSerializer) {
		this.configs = new HashMap<>(configs);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	public void setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public void setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * The time to wait when physically closing the producer (when {@link #stop()} or {@link #destroy()} is invoked.
	 * Specified in seconds; default {@value #DEFAULT_PHYSICAL_CLOSE_TIMEOUT}.
	 * @param physicalCloseTimeout the timeout in seconds.
	 * @since 1.0.7
	 */
	public void setPhysicalCloseTimeout(int physicalCloseTimeout) {
		this.physicalCloseTimeout = physicalCloseTimeout;
	}

	/**
	 * Set the transactional.id prefix.
	 * @param transactionIdPrefix the prefix.
	 * @since 1.3
	 */
	public void setTransactionIdPrefix(String transactionIdPrefix) {
		Assert.notNull(transactionIdPrefix, "'transactionIdPrefix' cannot be null");
		this.transactionIdPrefix = transactionIdPrefix;
	}

	/**
	 * Set to false to revert to the previous behavior of a simple incrementing
	 * trasactional.id suffix for each producer instead of maintaining a producer
	 * for each group/topic/partition.
	 * @param producerPerConsumerPartition false to revert.
	 * @since 1.3.7
	 */
	public void setProducerPerConsumerPartition(boolean producerPerConsumerPartition) {
		this.producerPerConsumerPartition = producerPerConsumerPartition;
	}

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 1.3
	 */
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public boolean transactionCapable() {
		return this.transactionIdPrefix != null;
	}

	@SuppressWarnings("resource")
	@Override
	public void destroy() throws Exception { //NOSONAR
		CloseSafeProducer<K, V> producer = this.producer;
		this.producer = null;
		if (producer != null) {
			producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
		}
		producer = this.cache.poll();
		while (producer != null) {
			try {
				producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
			}
			catch (Exception e) {
				logger.error("Exception while closing producer", e);
			}
			producer = this.cache.poll();
		}
		synchronized (this.consumerProducers) {
			for (Entry<String, CloseSafeProducer<K, V>> entry : this.consumerProducers.entrySet()) {
				entry.getValue().delegate
							.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
			}
			this.consumerProducers.clear();
		}
	}

	@Override
	public void start() {
		this.running = true;
	}


	@Override
	public void stop() {
		try {
			destroy();
			this.running = false;
		}
		catch (Exception e) {
			logger.error("Exception while closing producer", e);
		}
	}


	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public Producer<K, V> createProducer() {
		if (this.transactionIdPrefix != null) {
			if (this.producerPerConsumerPartition) {
				return createTransactionalProducerForPartition();
			}
			else {
				return createTransactionalProducer();
			}
		}
		if (this.producer == null) {
			synchronized (this) {
				if (this.producer == null) {
					this.producer = new CloseSafeProducer<K, V>(createKafkaProducer());
				}
			}
		}
		return this.producer;
	}

	/**
	 * Subclasses must return a raw producer which will be wrapped in a
	 * {@link CloseSafeProducer}.
	 * @return the producer.
	 */
	protected Producer<K, V> createKafkaProducer() {
		return new KafkaProducer<K, V>(this.configs, this.keySerializer, this.valueSerializer);
	}

	private Producer<K, V> createTransactionalProducerForPartition() {
		String suffix = TransactionSupport.getTransactionIdSuffix();
		if (suffix == null) {
			return createTransactionalProducer();
		}
		else {
			synchronized (this.consumerProducers) {
				if (!this.consumerProducers.containsKey(suffix)) {
					CloseSafeProducer<K, V> newProducer = doCreateTxProducer(suffix);
					this.consumerProducers.put(suffix, newProducer);
					return newProducer;
				}
				else {
					return this.consumerProducers.get(suffix);
				}
			}
		}
	}

	/**
	 * Subclasses must return a producer from the {@link #getCache()} or a
	 * new raw producer wrapped in a {@link CloseSafeProducer}.
	 * @return the producer - cannot be null.
	 * @since 1.3
	 */
	protected Producer<K, V> createTransactionalProducer() {
		Producer<K, V> producer = this.cache.poll();
		if (producer == null) {
			return doCreateTxProducer("" + this.transactionIdSuffix.getAndIncrement());
		}
		else {
			return producer;
		}
	}

	private CloseSafeProducer<K, V> doCreateTxProducer(String suffix) {
		Producer<K, V> producer;
		Map<String, Object> configs = new HashMap<>(this.configs);
		configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.transactionIdPrefix + suffix);
		producer = new KafkaProducer<K, V>(configs, this.keySerializer, this.valueSerializer);
		producer.initTransactions();
		return new CloseSafeProducer<K, V>(producer, this.cache, this.consumerProducers);
	}

	protected BlockingQueue<CloseSafeProducer<K, V>> getCache() {
		return this.cache;
	}

	public void closeProducerFor(String transactionIdSuffix) {
		if (this.producerPerConsumerPartition) {
			synchronized (this.consumerProducers) {
				CloseSafeProducer<K, V> removed = this.consumerProducers.remove(transactionIdSuffix);
				if (removed != null) {
					removed.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
				}
			}
		}
	}

	/**
	 * A wrapper class for the delegate.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	protected static class CloseSafeProducer<K, V> implements Producer<K, V> {

		private final Producer<K, V> delegate;

		private final BlockingQueue<CloseSafeProducer<K, V>> cache;

		private final Map<String, CloseSafeProducer<K, V>> consumerProducers;

		private volatile boolean txFailed;

		CloseSafeProducer(Producer<K, V> delegate) {
			this(delegate, null, null);
			Assert.isTrue(!(delegate instanceof CloseSafeProducer), "Cannot double-wrap a producer");
		}

		CloseSafeProducer(Producer<K, V> delegate, BlockingQueue<CloseSafeProducer<K, V>> cache) {
			this(delegate, cache, null);
		}

		CloseSafeProducer(Producer<K, V> delegate, BlockingQueue<CloseSafeProducer<K, V>> cache,
				Map<String, CloseSafeProducer<K, V>> consumerProducers) {
			this.delegate = delegate;
			this.cache = cache;
			this.consumerProducers = consumerProducers;
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
		public void initTransactions() {
			this.delegate.initTransactions();
		}

		@Override
		public void beginTransaction() throws ProducerFencedException {
			try {
				this.delegate.beginTransaction();
			}
			catch (RuntimeException e) {
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
				throws ProducerFencedException {
			this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
		}

		@Override
		public void commitTransaction() throws ProducerFencedException {
			try {
				this.delegate.commitTransaction();
			}
			catch (RuntimeException e) {
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void abortTransaction() throws ProducerFencedException {
			try {
				this.delegate.abortTransaction();
			}
			catch (RuntimeException e) {
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void close() {
			if (this.cache != null) {
				if (this.txFailed) {
					logger.warn("Error during transactional operation; producer removed from cache; possible cause: "
							+ "broker restarted during transaction");

					this.delegate.close();
					if (this.consumerProducers != null) {
						removeConsumerProducer();
					}
				}
				else {
					synchronized (this) {
						if (!this.cache.contains(this)) {
							this.cache.offer(this);
						}
					}
				}
			}
		}

		@Override
		public void close(long timeout, TimeUnit unit) {
			close();
		}

		private void removeConsumerProducer() {
			synchronized (this.consumerProducers) {
				Iterator<Entry<String, CloseSafeProducer<K, V>>> iterator = this.consumerProducers.entrySet()
						.iterator();
				while (iterator.hasNext()) {
					if (iterator.next().getValue().equals(this)) {
						iterator.remove();
						break;
					}
				}
			}
		}

		@Override
		public String toString() {
			return "CloseSafeProducer [delegate=" + this.delegate + "]";
		}

	}

}
