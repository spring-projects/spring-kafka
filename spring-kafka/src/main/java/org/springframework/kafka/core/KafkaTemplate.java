/*
 * Copyright 2015-2022 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.micrometer.MicrometerHolder;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;


/**
 * A template for executing high-level operations. When used with a
 * {@link DefaultKafkaProducerFactory}, the template is thread-safe. The producer factory
 * and {@link org.apache.kafka.clients.producer.KafkaProducer} ensure this; refer to their
 * respective javadocs.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Biju Kunjummen
 * @author Endika Gutierrez
 * @author Thomas Strauß
 * @author Soby Chacko
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V>, ApplicationContextAware, BeanNameAware,
		ApplicationListener<ContextStoppedEvent>, DisposableBean {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); //NOSONAR

	private final ProducerFactory<K, V> producerFactory;

	private final boolean customProducerFactory;

	private final boolean autoFlush;

	private final boolean transactional;

	private final ThreadLocal<Producer<K, V>> producers = new ThreadLocal<>();

	private final Map<String, String> micrometerTags = new HashMap<>();

	private String beanName = "kafkaTemplate";

	private ApplicationContext applicationContext;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private String defaultTopic;

	private ProducerListener<K, V> producerListener = new LoggingProducerListener<K, V>();

	private String transactionIdPrefix;

	private Duration closeTimeout = ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT;

	private boolean allowNonTransactional;

	private boolean converterSet;

	private ConsumerFactory<K, V> consumerFactory;

	private volatile boolean micrometerEnabled = true;

	private volatile MicrometerHolder micrometerHolder;

	private ProducerInterceptor<K, V> producerInterceptor;

	/**
	 * Create an instance using the supplied producer factory and autoFlush false.
	 * @param producerFactory the producer factory.
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory) {
		this(producerFactory, false);
	}

	/**
	 * Create an instance using the supplied producer factory and properties, with
	 * autoFlush false. If the configOverrides is not null or empty, a new
	 * {@link DefaultKafkaProducerFactory} will be created with merged producer properties
	 * with the overrides being applied after the supplied factory's properties.
	 * @param producerFactory the producer factory.
	 * @param configOverrides producer configuration properties to override.
	 * @since 2.5
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory, @Nullable Map<String, Object> configOverrides) {
		this(producerFactory, false, configOverrides);
	}

	/**
	 * Create an instance using the supplied producer factory and autoFlush setting.
	 * <p>
	 * Set autoFlush to {@code true} if you have configured the producer's
	 * {@code linger.ms} to a non-default value and wish send operations on this template
	 * to occur immediately, regardless of that setting, or if you wish to block until the
	 * broker has acknowledged receipt according to the producer's {@code acks} property.
	 * @param producerFactory the producer factory.
	 * @param autoFlush true to flush after each send.
	 * @see Producer#flush()
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush) {
		this(producerFactory, autoFlush, null);
	}

	/**
	 * Create an instance using the supplied producer factory and autoFlush setting.
	 * <p>
	 * Set autoFlush to {@code true} if you have configured the producer's
	 * {@code linger.ms} to a non-default value and wish send operations on this template
	 * to occur immediately, regardless of that setting, or if you wish to block until the
	 * broker has acknowledged receipt according to the producer's {@code acks} property.
	 * If the configOverrides is not null or empty, a new
	 * {@link ProducerFactory} will be created using
	 * {@link org.springframework.kafka.core.ProducerFactory#copyWithConfigurationOverride(java.util.Map)}
	 * The factory shall apply the overrides after the supplied factory's properties.
	 * The {@link org.springframework.kafka.core.ProducerPostProcessor}s from the
	 * original factory are copied over to keep instrumentation alive.
	 * Registered {@link org.springframework.kafka.core.ProducerFactory.Listener}s are
	 * also added to the new factory. If the factory implementation does not support
	 * the copy operation, a generic copy of the ProducerFactory is created which will
	 * be of type
	 * DefaultKafkaProducerFactory.
	 * @param producerFactory the producer factory.
	 * @param autoFlush true to flush after each send.
	 * @param configOverrides producer configuration properties to override.
	 * @since 2.5
	 * @see Producer#flush()
	 */
	public KafkaTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush,
			@Nullable Map<String, Object> configOverrides) {

		Assert.notNull(producerFactory, "'producerFactory' cannot be null");
		this.autoFlush = autoFlush;
		this.micrometerEnabled = KafkaUtils.MICROMETER_PRESENT;
		this.customProducerFactory = configOverrides != null && configOverrides.size() > 0;
		if (this.customProducerFactory) {
			this.producerFactory = producerFactory.copyWithConfigurationOverride(configOverrides);
		}
		else {
			this.producerFactory = producerFactory;
		}
		this.transactional = this.producerFactory.transactionCapable();
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
		if (this.customProducerFactory) {
			((DefaultKafkaProducerFactory<K, V>) this.producerFactory).setApplicationContext(applicationContext);
		}
	}

	/**
	 * The default topic for send methods where a topic is not
	 * provided.
	 * @return the topic.
	 */
	public String getDefaultTopic() {
		return this.defaultTopic;
	}

	/**
	 * Set the default topic for send methods where a topic is not
	 * provided.
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
	public void setProducerListener(@Nullable ProducerListener<K, V> producerListener) {
		this.producerListener = producerListener;
	}

	/**
	 * Return the message converter.
	 * @return the message converter.
	 */
	public RecordMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set the message converter to use.
	 * @param messageConverter the message converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.messageConverter = messageConverter;
		this.converterSet = true;
	}

	/**
	 * Set the {@link SmartMessageConverter} to use with the default
	 * {@link MessagingMessageConverter}. Not allowed when a custom
	 * {@link #setMessageConverter(RecordMessageConverter) messageConverter} is provided.
	 * @param messageConverter the converter.
	 * @since 2.7.1
	 */
	public void setMessagingConverter(SmartMessageConverter messageConverter) {
		Assert.isTrue(!this.converterSet, "Cannot set the SmartMessageConverter when setting the messageConverter, "
				+ "add the SmartConverter to the message converter instead");
		((MessagingMessageConverter) this.messageConverter).setMessagingConverter(messageConverter);
	}


	@Override
	public boolean isTransactional() {
		return this.transactional;
	}

	public String getTransactionIdPrefix() {
		return this.transactionIdPrefix;
	}

	/**
	 * Set a transaction id prefix to override the prefix in the producer factory.
	 * @param transactionIdPrefix the prefix.
	 * @since 2.3
	 */
	public void setTransactionIdPrefix(String transactionIdPrefix) {
		this.transactionIdPrefix = transactionIdPrefix;
	}

	/**
	 * Set the maximum time to wait when closing a producer; default 5 seconds.
	 * @param closeTimeout the close timeout.
	 * @since 2.1.14
	 */
	public void setCloseTimeout(Duration closeTimeout) {
		Assert.notNull(closeTimeout, "'closeTimeout' cannot be null");
		this.closeTimeout = closeTimeout;
	}

	/**
	 * Set to true to allow a non-transactional send when the template is transactional.
	 * @param allowNonTransactional true to allow.
	 * @since 2.4.3
	 */
	public void setAllowNonTransactional(boolean allowNonTransactional) {
		this.allowNonTransactional = allowNonTransactional;
	}

	@Override
	public boolean isAllowNonTransactional() {
		return this.allowNonTransactional;
	}

	/**
	 * Set to false to disable micrometer timers, if micrometer is on the class path.
	 * @param micrometerEnabled false to disable.
	 * @since 2.5
	 */
	public void setMicrometerEnabled(boolean micrometerEnabled) {
		this.micrometerEnabled = micrometerEnabled;
	}

	/**
	 * Set additional tags for the Micrometer listener timers.
	 * @param tags the tags.
	 * @since 2.5
	 */
	public void setMicrometerTags(Map<String, String> tags) {
		if (tags != null) {
			this.micrometerTags.putAll(tags);
		}
	}

	/**
	 * Return the producer factory used by this template.
	 * @return the factory.
	 * @since 2.2.5
	 */
	@Override
	public ProducerFactory<K, V> getProducerFactory() {
		return this.producerFactory;
	}

	/**
	 * Return the producer factory used by this template based on the topic.
	 * The default implementation returns the only producer factory.
	 * @param topic the topic.
	 * @return the factory.
	 * @since 2.5
	 */
	protected ProducerFactory<K, V> getProducerFactory(String topic) {
		return this.producerFactory;
	}

	/**
	 * Set a consumer factory for receive operations.
	 * @param consumerFactory the consumer factory.
	 * @since 2.8
	 */
	public void setConsumerFactory(ConsumerFactory<K, V> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	/**
	 * Set a producer interceptor on this template.
	 * @param producerInterceptor the producer interceptor
	 * @since 3.0
	 */
	public void setProducerInterceptor(ProducerInterceptor<K, V> producerInterceptor) {
		this.producerInterceptor = producerInterceptor;
	}

	@Override
	public void onApplicationEvent(ContextStoppedEvent event) {
		if (this.customProducerFactory) {
			((DefaultKafkaProducerFactory<K, V>) this.producerFactory).onApplicationEvent(event);
		}
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(@Nullable V data) {
		return send(this.defaultTopic, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(K key, @Nullable V data) {
		return send(this.defaultTopic, key, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, @Nullable V data) {
		return send(this.defaultTopic, partition, key, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, Long timestamp, K key, @Nullable V data) {
		return send(this.defaultTopic, partition, timestamp, key, data);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, @Nullable V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, K key, @Nullable V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, @Nullable V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, Long timestamp, K key,
			@Nullable V data) {

		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, data);
		return doSend(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> send(ProducerRecord<K, V> record) {
		Assert.notNull(record, "'record' cannot be null");
		return doSend(record);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ListenableFuture<SendResult<K, V>> send(Message<?> message) {
		ProducerRecord<?, ?> producerRecord = this.messageConverter.fromMessage(message, this.defaultTopic);
		if (!producerRecord.headers().iterator().hasNext()) { // possibly no Jackson
			byte[] correlationId = message.getHeaders().get(KafkaHeaders.CORRELATION_ID, byte[].class);
			if (correlationId != null) {
				producerRecord.headers().add(KafkaHeaders.CORRELATION_ID, correlationId);
			}
		}
		return doSend((ProducerRecord<K, V>) producerRecord);
	}


	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		Producer<K, V> producer = getTheProducer();
		try {
			return producer.partitionsFor(topic);
		}
		finally {
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		Producer<K, V> producer = getTheProducer();
		try {
			return producer.metrics();
		}
		finally {
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public <T> T execute(ProducerCallback<K, V, T> callback) {
		Assert.notNull(callback, "'callback' cannot be null");
		Producer<K, V> producer = getTheProducer();
		try {
			return callback.doInKafka(producer);
		}
		finally {
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public <T> T executeInTransaction(OperationsCallback<K, V, T> callback) {
		Assert.notNull(callback, "'callback' cannot be null");
		Assert.state(this.transactional, "Producer factory does not support transactions");
		Producer<K, V> producer = this.producers.get();
		Assert.state(producer == null, "Nested calls to 'executeInTransaction' are not allowed");
		String transactionIdSuffix;
		if (this.producerFactory.isProducerPerConsumerPartition()) {
			transactionIdSuffix = TransactionSupport.getTransactionIdSuffix();
			TransactionSupport.clearTransactionIdSuffix();
		}
		else {
			transactionIdSuffix = null;
		}

		producer = this.producerFactory.createProducer(this.transactionIdPrefix);

		try {
			producer.beginTransaction();
		}
		catch (Exception e) {
			closeProducer(producer, false);
			throw e;
		}

		this.producers.set(producer);
		try {
			T result = callback.doInOperations(this);
			try {
				producer.commitTransaction();
			}
			catch (Exception e) {
				throw new SkipAbortException(e);
			}
			return result;
		}
		catch (SkipAbortException e) { // NOSONAR - exception flow control
			throw ((RuntimeException) e.getCause()); // NOSONAR - lost stack trace
		}
		catch (Exception e) {
			producer.abortTransaction();
			throw e;
		}
		finally {
			if (transactionIdSuffix != null) {
				TransactionSupport.setTransactionIdSuffix(transactionIdSuffix);
			}
			this.producers.remove();
			closeProducer(producer, false);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p><b>Note</b> It only makes sense to invoke this method if the
	 * {@link ProducerFactory} serves up a singleton producer (such as the
	 * {@link DefaultKafkaProducerFactory}).
	 */
	@Override
	public void flush() {
		Producer<K, V> producer = getTheProducer();
		try {
			producer.flush();
		}
		finally {
			closeProducer(producer, inTransaction());
		}
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
			ConsumerGroupMetadata groupMetadata) {

		producerForOffsets().sendOffsetsToTransaction(offsets, groupMetadata);
	}

	@Override
	@Nullable
	public ConsumerRecord<K, V> receive(String topic, int partition, long offset, Duration pollTimeout) {
		Properties props = oneOnly();
		try (Consumer<K, V> consumer = this.consumerFactory.createConsumer(null, null, null, props)) {
			TopicPartition topicPartition = new TopicPartition(topic, partition);
			return receiveOne(topicPartition, offset, pollTimeout, consumer);
		}
	}

	@Override
	public ConsumerRecords<K, V> receive(Collection<TopicPartitionOffset> requested, Duration pollTimeout) {
		Properties props = oneOnly();
		Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new LinkedHashMap<>();
		try (Consumer<K, V> consumer = this.consumerFactory.createConsumer(null, null, null, props)) {
			requested.forEach(tpo -> {
				ConsumerRecord<K, V> one = receiveOne(tpo.getTopicPartition(), tpo.getOffset(), pollTimeout, consumer);
				records.computeIfAbsent(tpo.getTopicPartition(), tp -> new ArrayList<>()).add(one);
			});
			return new ConsumerRecords<>(records);
		}
	}

	private Properties oneOnly() {
		Assert.notNull(this.consumerFactory, "A consumerFactory is required");
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		return props;
	}

	@Nullable
	private ConsumerRecord<K, V> receiveOne(TopicPartition topicPartition, long offset, Duration pollTimeout,
			Consumer<K, V> consumer) {

		consumer.assign(Collections.singletonList(topicPartition));
		consumer.seek(topicPartition, offset);
		ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
		if (records.count() == 1) {
			return records.iterator().next();
		}
		return null;
	}

	private Producer<K, V> producerForOffsets() {
		Producer<K, V> producer = this.producers.get();
		if (producer == null) {
			@SuppressWarnings("unchecked")
			KafkaResourceHolder<K, V> resourceHolder = (KafkaResourceHolder<K, V>) TransactionSynchronizationManager
					.getResource(this.producerFactory);
			Assert.isTrue(resourceHolder != null, "No transaction in process");
			producer = resourceHolder.getProducer();
		}
		return producer;
	}

	protected void closeProducer(Producer<K, V> producer, boolean inTx) {
		if (!inTx) {
			producer.close(this.closeTimeout);
		}
	}

	/**
	 * Send the producer record.
	 * @param producerRecord the producer record.
	 * @return a Future for the {@link org.apache.kafka.clients.producer.RecordMetadata
	 * RecordMetadata}.
	 */
	protected ListenableFuture<SendResult<K, V>> doSend(final ProducerRecord<K, V> producerRecord) {
		final Producer<K, V> producer = getTheProducer(producerRecord.topic());
		this.logger.trace(() -> "Sending: " + producerRecord);
		final SettableListenableFuture<SendResult<K, V>> future = new SettableListenableFuture<>();
		Object sample = null;
		if (this.micrometerEnabled && this.micrometerHolder == null) {
			this.micrometerHolder = obtainMicrometerHolder();
		}
		if (this.micrometerHolder != null) {
			sample = this.micrometerHolder.start();
		}
		if (this.producerInterceptor != null) {
			this.producerInterceptor.onSend(producerRecord);
		}
		Future<RecordMetadata> sendFuture =
				producer.send(producerRecord, buildCallback(producerRecord, producer, future, sample));
		// May be an immediate failure
		if (sendFuture.isDone()) {
			try {
				sendFuture.get();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new KafkaException("Interrupted", e);
			}
			catch (ExecutionException e) {
				throw new KafkaException("Send failed", e.getCause()); // NOSONAR, stack trace
			}
		}
		if (this.autoFlush) {
			flush();
		}
		this.logger.trace(() -> "Sent: " + producerRecord);
		return future;
	}

	private Callback buildCallback(final ProducerRecord<K, V> producerRecord, final Producer<K, V> producer,
			final SettableListenableFuture<SendResult<K, V>> future, @Nullable Object sample) {

		return (metadata, exception) -> {
			try {
				if (this.producerInterceptor != null) {
					this.producerInterceptor.onAcknowledgement(metadata, exception);
				}
			}
			catch (Exception e) {
				KafkaTemplate.this.logger.warn(e, () ->  "Error executing interceptor onAcknowledgement callback");
			}
			try {
				if (exception == null) {
					if (sample != null) {
						this.micrometerHolder.success(sample);
					}
					future.set(new SendResult<>(producerRecord, metadata));
					if (KafkaTemplate.this.producerListener != null) {
						KafkaTemplate.this.producerListener.onSuccess(producerRecord, metadata);
					}
					KafkaTemplate.this.logger.trace(() -> "Sent ok: " + producerRecord + ", metadata: " + metadata);
				}
				else {
					if (sample != null) {
						this.micrometerHolder.failure(sample, exception.getClass().getSimpleName());
					}
					future.setException(new KafkaProducerException(producerRecord, "Failed to send", exception));
					if (KafkaTemplate.this.producerListener != null) {
						KafkaTemplate.this.producerListener.onError(producerRecord, metadata, exception);
					}
					KafkaTemplate.this.logger.debug(exception, () -> "Failed to send: " + producerRecord);
				}
			}
			finally {
				if (!KafkaTemplate.this.transactional) {
					closeProducer(producer, false);
				}
			}
		};
	}


	/**
	 * Return true if the template is currently running in a transaction on the calling
	 * thread.
	 * @return true if a transaction is running.
	 * @since 2.2.1
	 */
	@Override
	public boolean inTransaction() {
		return this.transactional && (this.producers.get() != null
				|| TransactionSynchronizationManager.getResource(this.producerFactory) != null
				|| TransactionSynchronizationManager.isActualTransactionActive());
	}

	private Producer<K, V> getTheProducer() {
		return getTheProducer(null);
	}

	protected Producer<K, V> getTheProducer(@SuppressWarnings("unused") @Nullable String topic) {
		boolean transactionalProducer = this.transactional;
		if (transactionalProducer) {
			boolean inTransaction = inTransaction();
			Assert.state(this.allowNonTransactional || inTransaction,
					"No transaction is in process; "
						+ "possible solutions: run the template operation within the scope of a "
						+ "template.executeInTransaction() operation, start a transaction with @Transactional "
						+ "before invoking the template method, "
						+ "run in a transaction started by a listener container when consuming a record");
			if (!inTransaction) {
				transactionalProducer = false;
			}
		}
		if (transactionalProducer) {
			Producer<K, V> producer = this.producers.get();
			if (producer != null) {
				return producer;
			}
			KafkaResourceHolder<K, V> holder = ProducerFactoryUtils
					.getTransactionalResourceHolder(this.producerFactory, this.transactionIdPrefix, this.closeTimeout);
			return holder.getProducer();
		}
		else if (this.allowNonTransactional) {
			return this.producerFactory.createNonTransactionalProducer();
		}
		else if (topic == null) {
			return this.producerFactory.createProducer();
		}
		else {
			return getProducerFactory(topic).createProducer();
		}
	}

	@Nullable
	private MicrometerHolder obtainMicrometerHolder() {
		MicrometerHolder holder = null;
		try {
			if (KafkaUtils.MICROMETER_PRESENT) {
				holder = new MicrometerHolder(this.applicationContext, this.beanName,
						"spring.kafka.template", "KafkaTemplate Timer",
						this.micrometerTags);
			}
		}
		catch (@SuppressWarnings("unused") IllegalStateException ex) {
			this.micrometerEnabled = false;
		}
		return holder;
	}

	@Override
	public void destroy() {
		if (this.micrometerHolder != null) {
			this.micrometerHolder.destroy();
		}
		if (this.customProducerFactory) {
			((DefaultKafkaProducerFactory<K, V>) this.producerFactory).destroy();
		}
		if (this.producerInterceptor != null) {
			this.producerInterceptor.close();
		}
	}

	@SuppressWarnings("serial")
	private static final class SkipAbortException extends RuntimeException {

		SkipAbortException(Throwable cause) {
			super(cause);
		}

	}

}
