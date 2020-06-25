/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.kafka.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
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
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The {@link ProducerFactory} implementation for a {@code singleton} shared {@link Producer} instance.
 * <p>
 * This implementation will return the same {@link Producer} instance (if transactions are
 * not enabled) for the provided {@link Map} {@code configs} and optional {@link Serializer}
 * implementations on each {@link #createProducer()} invocation.
 * <p>
 * If you are using {@link Serializer}s that have no-arg constructors and require no setup, then simplest to
 * specify {@link Serializer} classes against {@link ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG} and
 * {@link ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG} keys in the {@code configs} passed to the
 * {@link DefaultKafkaProducerFactory} constructor.
 * <p>
 * If that is not possible, but you are sure that at least one of the following is true:
 * <ul>
 *     <li>only one {@link Producer} will use the {@link Serializer}s</li>
 *     <li>you are using {@link Serializer}s that may be shared between {@link Producer} instances (and specifically
 *     that their close() method is a no-op)</li>
 *     <li>you are certain that there is no risk of any single {@link Producer} being closed while other
 *     {@link Producer} instances with the same {@link Serializer}s are in use</li>
 * </ul>
 * then you can pass in {@link Serializer} instances for one or both of the key and value serializers.
 * <p>
 * If none of the above is true then you may provide a {@link Supplier} function for one or both {@link Serializer}s
 * which will be used to obtain {@link Serializer}(s) each time a {@link Producer} is created by the factory.
 * <p>
 * The {@link Producer} is wrapped and the underlying {@link KafkaProducer} instance is
 * not actually closed when {@link Producer#close()} is invoked. The {@link KafkaProducer}
 * is physically closed when {@link DisposableBean#destroy()} is invoked or when the
 * application context publishes a {@link ContextStoppedEvent}. You can also invoke
 * {@link #reset()}.
 * <p>
 * Setting {@link #setTransactionIdPrefix(String)} enables transactions; in which case, a
 * cache of producers is maintained; closing a producer returns it to the cache. The
 * producers are closed and the cache is cleared when the factory is destroyed, the
 * application context stopped, or the {@link #reset()} method is called.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 * @author Nakul Mishra
 * @author Artem Bilan
 * @author Chris Gilbert
 */
public class DefaultKafkaProducerFactory<K, V> extends KafkaResourceFactory
		implements ProducerFactory<K, V>, ApplicationContextAware,
			BeanNameAware, ApplicationListener<ContextStoppedEvent>, DisposableBean {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(DefaultKafkaProducerFactory.class));

	private final Map<String, Object> configs;

	private final AtomicInteger transactionIdSuffix = new AtomicInteger();

	private final Map<String, BlockingQueue<CloseSafeProducer<K, V>>> cache = new ConcurrentHashMap<>();

	private final Map<String, CloseSafeProducer<K, V>> consumerProducers = new HashMap<>();

	private final ThreadLocal<CloseSafeProducer<K, V>> threadBoundProducers = new ThreadLocal<>();

	private final ThreadLocal<Integer> threadBoundProducerEpochs = new ThreadLocal<>();

	private final AtomicInteger epoch = new AtomicInteger();

	private final AtomicInteger clientIdCounter = new AtomicInteger();

	private final List<Listener<K, V>> listeners = new ArrayList<>();

	private final List<ProducerPostProcessor<K, V>> postProcessors = new ArrayList<>();

	private Supplier<Serializer<K>> keySerializerSupplier;

	private Supplier<Serializer<V>> valueSerializerSupplier;

	private Duration physicalCloseTimeout = DEFAULT_PHYSICAL_CLOSE_TIMEOUT;

	private String transactionIdPrefix;

	private ApplicationContext applicationContext;

	private String beanName = "not.managed.by.Spring";

	private boolean producerPerConsumerPartition = true;

	private boolean producerPerThread;

	private String clientIdPrefix;

	private volatile CloseSafeProducer<K, V> producer;

	/**
	 * Construct a factory with the provided configuration.
	 * @param configs the configuration.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs) {
		this(configs, () -> null, () -> null);
	}

	/**
	 * Construct a factory with the provided configuration and {@link Serializer}s.
	 * Also configures a {@link #transactionIdPrefix} as a value from the
	 * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} if provided.
	 * This config is going to be overridden with a suffix for target {@link Producer} instance.
	 * @param configs the configuration.
	 * @param keySerializer the key {@link Serializer}.
	 * @param valueSerializer the value {@link Serializer}.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs,
			@Nullable Serializer<K> keySerializer,
			@Nullable Serializer<V> valueSerializer) {

		this(configs, () -> keySerializer, () -> valueSerializer);
	}

	/**
	 * Construct a factory with the provided configuration and {@link Serializer} Suppliers.
	 * Also configures a {@link #transactionIdPrefix} as a value from the
	 * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} if provided.
	 * This config is going to be overridden with a suffix for target {@link Producer} instance.
	 * @param configs the configuration.
	 * @param keySerializerSupplier the key {@link Serializer} supplier function.
	 * @param valueSerializerSupplier the value {@link Serializer} supplier function.
	 * @since 2.3
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs,
			@Nullable Supplier<Serializer<K>> keySerializerSupplier,
			@Nullable Supplier<Serializer<V>> valueSerializerSupplier) {

		this.configs = new HashMap<>(configs);
		this.keySerializerSupplier = keySerializerSupplier == null ? () -> null : keySerializerSupplier;
		this.valueSerializerSupplier = valueSerializerSupplier == null ? () -> null : valueSerializerSupplier;
		if (this.clientIdPrefix == null && configs.get(ProducerConfig.CLIENT_ID_CONFIG) instanceof String) {
			this.clientIdPrefix = (String) configs.get(ProducerConfig.CLIENT_ID_CONFIG);
		}

		String txId = (String) this.configs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
		if (StringUtils.hasText(txId)) {
			setTransactionIdPrefix(txId);
			this.configs.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Set a key serializer.
	 * @param keySerializer the key serializer.
	 */
	public void setKeySerializer(@Nullable Serializer<K> keySerializer) {
		this.keySerializerSupplier = () -> keySerializer;
	}

	/**
	 * Set a value serializer.
	 * @param valueSerializer the value serializer.
	 */
	public void setValueSerializer(@Nullable Serializer<V> valueSerializer) {
		this.valueSerializerSupplier = () -> valueSerializer;
	}

	/**
	 * The time to wait when physically closing the producer via the factory rather than
	 * closing the producer itself (when {@link #reset()}, {@link #destroy()
	 * #closeProducerFor(String)}, or {@link #closeThreadBoundProducer()} are invoked).
	 * Specified in seconds; default {@link #DEFAULT_PHYSICAL_CLOSE_TIMEOUT}.
	 * @param physicalCloseTimeout the timeout in seconds.
	 * @since 1.0.7
	 */
	public void setPhysicalCloseTimeout(int physicalCloseTimeout) {
		this.physicalCloseTimeout = Duration.ofSeconds(physicalCloseTimeout);
	}

	/**
	 * Get the physical close timeout.
	 * @return the timeout.
	 * @since 2.5
	 */
	@Override
	public Duration getPhysicalCloseTimeout() {
		return this.physicalCloseTimeout;
	}

	/**
	 * Set a prefix for the {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} config. By
	 * default a {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} value from configs is used
	 * as a prefix in the target producer configs.
	 * @param transactionIdPrefix the prefix.
	 * @since 1.3
	 */
	public final void setTransactionIdPrefix(String transactionIdPrefix) {
		Assert.notNull(transactionIdPrefix, "'transactionIdPrefix' cannot be null");
		this.transactionIdPrefix = transactionIdPrefix;
		enableIdempotentBehaviour();
	}

	@Override
	public @Nullable String getTransactionIdPrefix() {
		return this.transactionIdPrefix;
	}

	/**
	 * Set to true to create a producer per thread instead of singleton that is shared by
	 * all clients. Clients <b>must</b> call {@link #closeThreadBoundProducer()} to
	 * physically close the producer when it is no longer needed. These producers will not
	 * be closed by {@link #destroy()} or {@link #reset()}.
	 * @param producerPerThread true for a producer per thread.
	 * @since 2.3
	 * @see #closeThreadBoundProducer()
	 */
	public void setProducerPerThread(boolean producerPerThread) {
		this.producerPerThread = producerPerThread;
	}

	@Override
	public boolean isProducerPerThread() {
		return this.producerPerThread;
	}

	/**
	 * Set to false to revert to the previous behavior of a simple incrementing
	 * transactional.id suffix for each producer instead of maintaining a producer
	 * for each group/topic/partition.
	 * @param producerPerConsumerPartition false to revert.
	 * @since 1.3.7
	 */
	public void setProducerPerConsumerPartition(boolean producerPerConsumerPartition) {
		this.producerPerConsumerPartition = producerPerConsumerPartition;
	}

	/**
	 * Return the producerPerConsumerPartition.
	 * @return the producerPerConsumerPartition.
	 * @since 1.3.8
	 */
	@Override
	public boolean isProducerPerConsumerPartition() {
		return this.producerPerConsumerPartition;
	}

	@Override
	public Supplier<Serializer<K>> getKeySerializerSupplier() {
		return this.keySerializerSupplier;
	}

	@Override
	public Supplier<Serializer<V>> getValueSerializerSupplier() {
		return this.valueSerializerSupplier;
	}

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 1.3
	 */
	@Override
	public Map<String, Object> getConfigurationProperties() {
		Map<String, Object> configs2 = new HashMap<>(this.configs);
		checkBootstrap(configs2);
		return Collections.unmodifiableMap(configs2);
	}

	/**
	 * Get the current list of listeners.
	 * @return the listeners.
	 * @since 2.5
	 */
	@Override
	public List<Listener<K, V>> getListeners() {
		return Collections.unmodifiableList(this.listeners);
	}

	@Override
	public List<ProducerPostProcessor<K, V>> getPostProcessors() {
		return Collections.unmodifiableList(this.postProcessors);
	}

	/**
	 * Add a listener.
	 * @param listener the listener.
	 * @since 2.5
	 */
	@Override
	public void addListener(Listener<K, V> listener) {
		Assert.notNull(listener, "'listener' cannot be null");
		this.listeners.add(listener);
	}

	/**
	 * Add a listener at a specific index.
	 * @param index the index (list position).
	 * @param listener the listener.
	 * @since 2.5
	 */
	@Override
	public void addListener(int index, Listener<K, V> listener) {
		Assert.notNull(listener, "'listener' cannot be null");
		if (index >= this.listeners.size()) {
			this.listeners.add(listener);
		}
		else {
			this.listeners.add(index, listener);
		}
	}

	/**
	 * Remove a listener.
	 * @param listener the listener.
	 * @return true if removed.
	 * @since 2.5
	 */
	@Override
	public boolean removeListener(Listener<K, V> listener) {
		return this.listeners.remove(listener);
	}

	@Override
	public void addPostProcessor(ProducerPostProcessor<K, V> postProcessor) {
		Assert.notNull(postProcessor, "'postProcessor' cannot be null");
		this.postProcessors.add(postProcessor);
	}

	@Override
	public boolean removePostProcessor(ProducerPostProcessor<K, V> postProcessor) {
		return this.postProcessors.remove(postProcessor);
	}

	/**
	 * When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream.
	 */
	private void enableIdempotentBehaviour() {
		Object previousValue = this.configs.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		if (Boolean.FALSE.equals(previousValue)) {
			LOGGER.debug(() -> "The '" + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
					+ "' is set to false, may result in duplicate messages");
		}
	}

	@Override
	public boolean transactionCapable() {
		return this.transactionIdPrefix != null;
	}

	@SuppressWarnings("resource")
	@Override
	public void destroy() {
		CloseSafeProducer<K, V> producerToClose;
		synchronized (this) {
			producerToClose = this.producer;
			this.producer = null;
		}
		if (producerToClose != null) {
			producerToClose.closeDelegate(this.physicalCloseTimeout, this.listeners);
		}
		this.cache.values().forEach(queue -> {
			CloseSafeProducer<K, V> next = queue.poll();
			while (next != null) {
				try {
					next.closeDelegate(this.physicalCloseTimeout, this.listeners);
				}
				catch (Exception e) {
					LOGGER.error(e, "Exception while closing producer");
				}
				next = queue.poll();
			}
		});
		this.cache.clear();
		synchronized (this.consumerProducers) {
			this.consumerProducers.forEach(
					(k, v) -> v.closeDelegate(this.physicalCloseTimeout, this.listeners));
			this.consumerProducers.clear();
		}
		this.epoch.incrementAndGet();
	}

	@Override
	public void onApplicationEvent(ContextStoppedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			reset();
		}
	}

	/**
	 * Close the {@link Producer}(s) and clear the cache of transactional
	 * {@link Producer}(s).
	 * @since 2.2
	 */
	@Override
	public void reset() {
		try {
			destroy();
		}
		catch (Exception e) {
			LOGGER.error(e, "Exception while closing producer");
		}
	}

	@Override
	public Producer<K, V> createProducer() {
		return createProducer(this.transactionIdPrefix);
	}

	@Override
	public Producer<K, V> createProducer(@Nullable String txIdPrefixArg) {
		String txIdPrefix = txIdPrefixArg == null ? this.transactionIdPrefix : txIdPrefixArg;
		return doCreateProducer(txIdPrefix);
	}

	@Override
	public Producer<K, V> createNonTransactionalProducer() {
		return doCreateProducer(null);
	}

	private Producer<K, V> doCreateProducer(@Nullable String txIdPrefix) {
		if (txIdPrefix != null) {
			if (this.producerPerConsumerPartition) {
				return createTransactionalProducerForPartition(txIdPrefix);
			}
			else {
				return createTransactionalProducer(txIdPrefix);
			}
		}
		if (this.producerPerThread) {
			CloseSafeProducer<K, V> tlProducer = this.threadBoundProducers.get();
			if (this.threadBoundProducerEpochs.get() == null) {
				this.threadBoundProducerEpochs.set(this.epoch.get());
			}
			if (tlProducer != null && this.epoch.get() != this.threadBoundProducerEpochs.get()) {
				closeThreadBoundProducer();
				tlProducer = null;
			}
			if (tlProducer == null) {
				tlProducer = new CloseSafeProducer<>(createKafkaProducer(), this::removeProducer,
						this.physicalCloseTimeout, this.beanName);
				for (Listener<K, V> listener : this.listeners) {
					listener.producerAdded(tlProducer.clientId, tlProducer);
				}
				this.threadBoundProducers.set(tlProducer);
				this.threadBoundProducerEpochs.set(this.epoch.get());
			}
			return tlProducer;
		}
		synchronized (this) {
			if (this.producer == null) {
				this.producer = new CloseSafeProducer<>(createKafkaProducer(), this::removeProducer,
						this.physicalCloseTimeout, this.beanName);
				this.listeners.forEach(listener -> listener.producerAdded(this.producer.clientId, this.producer));
			}
			return this.producer;
		}
	}

	/**
	 * Subclasses must return a raw producer which will be wrapped in a {@link CloseSafeProducer}.
	 * @return the producer.
	 */
	protected Producer<K, V> createKafkaProducer() {
		Map<String, Object> newConfigs;
		if (this.clientIdPrefix == null) {
			newConfigs = new HashMap<>(this.configs);
		}
		else {
			newConfigs = new HashMap<>(this.configs);
			newConfigs.put(ProducerConfig.CLIENT_ID_CONFIG,
					this.clientIdPrefix + "-" + this.clientIdCounter.incrementAndGet());
		}
		checkBootstrap(newConfigs);
		return createRawProducer(newConfigs);
	}

	protected Producer<K, V> createTransactionalProducerForPartition() {
		return createTransactionalProducerForPartition(this.transactionIdPrefix);
	}

	protected Producer<K, V> createTransactionalProducerForPartition(String txIdPrefix) {
		String suffix = TransactionSupport.getTransactionIdSuffix();
		if (suffix == null) {
			return createTransactionalProducer(txIdPrefix);
		}
		else {
			synchronized (this.consumerProducers) {
				if (!this.consumerProducers.containsKey(suffix)) {
					CloseSafeProducer<K, V> newProducer = doCreateTxProducer(txIdPrefix, suffix,
							this::removeConsumerProducer);
					this.consumerProducers.put(suffix, newProducer);
					return newProducer;
				}
				else {
					return this.consumerProducers.get(suffix);
				}
			}
		}
	}

	private boolean removeConsumerProducer(CloseSafeProducer<K, V> producerToRemove, Duration timeout) {
		if (producerToRemove.closed) {
			synchronized (this.consumerProducers) {
				Iterator<Entry<String, CloseSafeProducer<K, V>>> iterator = this.consumerProducers.entrySet().iterator();
				while (iterator.hasNext()) {
					if (iterator.next().getValue().equals(producerToRemove)) {
						iterator.remove();
						producerToRemove.closeDelegate(timeout, this.listeners);
						return true;
					}
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Remove the single shared producer and a thread-bound instance if present.
	 * @param producerToRemove the producer.
	 * @param timeout the close timeout.
	 * @return always true.
	 * @since 2.2.13
	 */
	protected final synchronized boolean removeProducer(CloseSafeProducer<K, V> producerToRemove, Duration timeout) {
		if (producerToRemove.closed) {
			if (producerToRemove.equals(this.producer)) {
				this.producer = null;
				producerToRemove.closeDelegate(timeout, this.listeners);
			}
			this.threadBoundProducers.remove();
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Subclasses must return a producer from the {@link #getCache()} or a
	 * new raw producer wrapped in a {@link CloseSafeProducer}.
	 * @return the producer - cannot be null.
	 * @since 1.3
	 */
	protected Producer<K, V> createTransactionalProducer() {
		return createTransactionalProducer(this.transactionIdPrefix);
	}

	protected Producer<K, V> createTransactionalProducer(String txIdPrefix) {
		BlockingQueue<CloseSafeProducer<K, V>> queue = getCache(txIdPrefix);
		Assert.notNull(queue, () -> "No cache found for " + txIdPrefix);
		Producer<K, V> cachedProducer = queue.poll();
		if (cachedProducer == null) {
			return doCreateTxProducer(txIdPrefix, "" + this.transactionIdSuffix.getAndIncrement(), this::cacheReturner);
		}
		else {
			return cachedProducer;
		}
	}

	boolean cacheReturner(CloseSafeProducer<K, V> producerToRemove, Duration timeout) {
		if (producerToRemove.closed) {
			producerToRemove.closeDelegate(timeout, this.listeners);
			return true;
		}
		else {
			synchronized (this.cache) {
				BlockingQueue<CloseSafeProducer<K, V>> txIdCache = getCache(producerToRemove.txIdPrefix);
				if (txIdCache != null && !txIdCache.contains(producerToRemove) && !txIdCache.offer(producerToRemove)) {
					producerToRemove.closeDelegate(timeout, this.listeners);
					return true;
				}
			}
			return false;
		}
	}

	private CloseSafeProducer<K, V> doCreateTxProducer(String prefix, String suffix,
			BiPredicate<CloseSafeProducer<K, V>, Duration> remover) {

		Producer<K, V> newProducer;
		Map<String, Object> newProducerConfigs = new HashMap<>(this.configs);
		String txId = prefix + suffix;
		newProducerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
		if (this.clientIdPrefix != null) {
			newProducerConfigs.put(ProducerConfig.CLIENT_ID_CONFIG,
					this.clientIdPrefix + "-" + this.clientIdCounter.incrementAndGet());
		}
		checkBootstrap(newProducerConfigs);
		newProducer = createRawProducer(newProducerConfigs);
		newProducer.initTransactions();
		CloseSafeProducer<K, V> closeSafeProducer =
				new CloseSafeProducer<>(newProducer, remover, prefix, this.physicalCloseTimeout, this.beanName);
		this.listeners.forEach(listener -> listener.producerAdded(closeSafeProducer.clientId, closeSafeProducer));
		return closeSafeProducer;
	}

	protected Producer<K, V> createRawProducer(Map<String, Object> rawConfigs) {
		KafkaProducer<K, V> kafkaProducer =
				new KafkaProducer<>(rawConfigs, this.keySerializerSupplier.get(), this.valueSerializerSupplier.get());
		this.postProcessors.forEach(pp -> pp.apply(kafkaProducer));
		return kafkaProducer;
	}

	@Nullable
	protected BlockingQueue<CloseSafeProducer<K, V>> getCache() {
		return getCache(this.transactionIdPrefix);
	}

	@Nullable
	protected BlockingQueue<CloseSafeProducer<K, V>> getCache(String txIdPrefix) {
		if (txIdPrefix == null) {
			return null;
		}
		return this.cache.computeIfAbsent(txIdPrefix, txId -> new LinkedBlockingQueue<>());
	}

	@Override
	public void closeProducerFor(String suffix) {
		if (this.producerPerConsumerPartition) {
			synchronized (this.consumerProducers) {
				CloseSafeProducer<K, V> removed = this.consumerProducers.remove(suffix);
				if (removed != null) {
					removed.closeDelegate(this.physicalCloseTimeout, this.listeners);
				}
			}
		}
	}

	/**
	 * When using {@link #setProducerPerThread(boolean)} (true), call this method to close
	 * and release this thread's producer. Thread bound producers are <b>not</b> closed by
	 * {@link #destroy()} or {@link #reset()} methods.
	 * @since 2.3
	 * @see #setProducerPerThread(boolean)
	 */
	@Override
	public void closeThreadBoundProducer() {
		CloseSafeProducer<K, V> tlProducer = this.threadBoundProducers.get();
		if (tlProducer != null) {
			this.threadBoundProducers.remove();
			tlProducer.closeDelegate(this.physicalCloseTimeout, this.listeners);
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

		private static final Duration CLOSE_TIMEOUT_AFTER_TX_TIMEOUT = Duration.ofMillis(0);

		private final Producer<K, V> delegate;

		private final BiPredicate<CloseSafeProducer<K, V>, Duration> removeProducer;

		final String txIdPrefix; // NOSONAR

		private final Duration closeTimeout;

		final String clientId; // NOSONAR

		private volatile Exception producerFailed;

		volatile boolean closed; // NOSONAR

		CloseSafeProducer(Producer<K, V> delegate,
				BiPredicate<CloseSafeProducer<K, V>, Duration> removeConsumerProducer, Duration closeTimeout,
				String factoryName) {

			this(delegate, removeConsumerProducer, null, closeTimeout, factoryName);
		}

		CloseSafeProducer(Producer<K, V> delegate,
				BiPredicate<CloseSafeProducer<K, V>, Duration> removeProducer, @Nullable String txIdPrefix,
				Duration closeTimeout, String factoryName) {

			Assert.isTrue(!(delegate instanceof CloseSafeProducer), "Cannot double-wrap a producer");
			this.delegate = delegate;
			this.removeProducer = removeProducer;
			this.txIdPrefix = txIdPrefix;
			this.closeTimeout = closeTimeout;
			Map<MetricName, ? extends Metric> metrics = delegate.metrics();
			Iterator<MetricName> metricIterator = metrics.keySet().iterator();
			String id;
			if (metricIterator.hasNext()) {
				id = metricIterator.next().tags().get("client-id");
			}
			else {
				id = "unknown";
			}
			this.clientId = factoryName + "." + id;
			LOGGER.debug(() -> "Created new Producer: " + this);
		}

		Producer<K, V> getDelegate() {
			return this.delegate;
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			LOGGER.trace(() -> toString() + " send(" + record + ")");
			return this.delegate.send(record);
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			LOGGER.trace(() -> toString() + " send(" + record + ")");
			return this.delegate.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception instanceof OutOfOrderSequenceException) {
						CloseSafeProducer.this.producerFailed = exception;
						close(CloseSafeProducer.this.closeTimeout);
					}
					callback.onCompletion(metadata, exception);
				}

			});
		}

		@Override
		public void flush() {
			LOGGER.trace(() -> toString() + " flush()");
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
			LOGGER.debug(() -> toString() + " beginTransaction()");
			try {
				this.delegate.beginTransaction();
			}
			catch (RuntimeException e) {
				LOGGER.error(e, () -> "beginTransaction failed: " + this);
				this.producerFailed = e;
				throw e;
			}
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
				throws ProducerFencedException {

			LOGGER.trace(() -> toString() + " sendOffsetsToTransaction(" + offsets + ", " + consumerGroupId + ")");
			this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
				ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {

			LOGGER.trace(() -> toString() + " sendOffsetsToTransaction(" + offsets + ", " + groupMetadata + ")");
			this.delegate.sendOffsetsToTransaction(offsets, groupMetadata);
		}

		@Override
		public void commitTransaction() throws ProducerFencedException {
			LOGGER.debug(() -> toString() + " commitTransaction()");
			try {
				this.delegate.commitTransaction();
			}
			catch (RuntimeException e) {
				LOGGER.error(e, () -> "commitTransaction failed: " + this);
				this.producerFailed = e;
				throw e;
			}
		}

		@Override
		public void abortTransaction() throws ProducerFencedException {
			LOGGER.debug(() -> toString() + " abortTransaction()");
			if (this.producerFailed != null) {
				LOGGER.debug(() -> "abortTransaction ignored - previous txFailed: " + this.producerFailed.getMessage()
					+ ": " + this);
			}
			else {
				try {
					this.delegate.abortTransaction();
				}
				catch (RuntimeException e) {
					LOGGER.error(e, () -> "Abort failed: " + this);
					this.producerFailed = e;
					throw e;
				}
			}
		}

		@Override
		public void close() {
			close(null);
		}

		@Override
		public void close(@Nullable Duration timeout) {
			LOGGER.trace(() -> toString() + " close(" + (timeout == null ? "null" : timeout) + ")");
			if (!this.closed) {
				if (this.producerFailed != null) {
					LOGGER.warn(() -> "Error during some operation; producer removed from cache: " + this);
					this.closed = true;
					this.removeProducer.test(this, this.producerFailed instanceof TimeoutException
							? CLOSE_TIMEOUT_AFTER_TX_TIMEOUT
							: timeout);
				}
				else {
					this.closed = this.removeProducer.test(this, timeout);
				}
			}
		}

		void closeDelegate(Duration timeout, List<Listener<K, V>> listeners) {
			this.delegate.close(timeout == null ? this.closeTimeout : timeout);
			listeners.forEach(listener -> listener.producerRemoved(this.clientId, this));
			this.closed = true;
		}

		@Override
		public String toString() {
			return "CloseSafeProducer [delegate=" + this.delegate + "]";
		}

	}

}
