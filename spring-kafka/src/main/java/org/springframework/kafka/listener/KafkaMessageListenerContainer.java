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

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.context.ApplicationContext;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppingEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Builder;
import io.micrometer.core.instrument.Timer.Sample;

/**
 * Single-threaded Message listener container using the Java {@link Consumer} supporting
 * auto-partition assignment or user-configured assignment.
 * <p>
 * With the latter, initial partition offsets can be provided.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 * @author Marius Bogoevici
 * @author Martin Dam
 * @author Artem Bilan
 * @author Loic Talhouarne
 * @author Vladimir Tsanev
 * @author Chen Binbin
 * @author Yang Qiju
 * @author Tom van den Berge
 * @author Lukasz Kaminski
 */
public class KafkaMessageListenerContainer<K, V> // NOSONAR line count
		extends AbstractMessageListenerContainer<K, V> {

	private static final String UNUSED = "unused";

	private static final int DEFAULT_ACK_TIME = 5000;

	private static final boolean MICROMETER_PRESENT = ClassUtils.isPresent(
			"io.micrometer.core.instrument.MeterRegistry", KafkaMessageListenerContainer.class.getClassLoader());

	private final AbstractMessageListenerContainer<K, V> thisOrParentContainer;

	private final TopicPartitionOffset[] topicPartitions;

	private String clientIdSuffix;

	private Runnable emergencyStop = () -> stop(() -> {
		// NOSONAR
	});

	private volatile ListenerConsumer listenerConsumer;

	private volatile ListenableFuture<?> listenerConsumerFuture;

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties) {

		this(null, consumerFactory, containerProperties, (TopicPartitionOffset[]) null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions/initialOffsets.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 * @deprecated - the topicPartitions should be provided in the
	 * {@link ContainerProperties}.
	 */
	@Deprecated
	public KafkaMessageListenerContainer(ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties, TopicPartitionOffset... topicPartitions) {

		this(null, consumerFactory, containerProperties, topicPartitions);
	}

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param container a delegating container (if this is a sub-container).
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	KafkaMessageListenerContainer(AbstractMessageListenerContainer<K, V> container,
			ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties) {

		this(container, consumerFactory, containerProperties, (TopicPartitionOffset[]) null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions/initialOffsets.
	 * @param container a delegating container (if this is a sub-container).
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 * @deprecated in favor of
	 * {@link #KafkaMessageListenerContainer(AbstractMessageListenerContainer, ConsumerFactory, ContainerProperties, TopicPartitionOffset...)}
	 */
	@Deprecated
	KafkaMessageListenerContainer(AbstractMessageListenerContainer<K, V> container,
			ConsumerFactory<? super K, ? super V> consumerFactory, ContainerProperties containerProperties,
			org.springframework.kafka.support.TopicPartitionInitialOffset... topicPartitions) {

		super(consumerFactory, containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.thisOrParentContainer = container == null ? this : container;
		if (topicPartitions != null) {
			this.topicPartitions = Arrays.stream(topicPartitions)
					.map(org.springframework.kafka.support.TopicPartitionInitialOffset::toTPO)
					.toArray(TopicPartitionOffset[]::new);
		}
		else {
			this.topicPartitions = containerProperties.getTopicPartitionsToAssign();
		}
	}

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions/initialOffsets.
	 * @param container a delegating container (if this is a sub-container).
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	KafkaMessageListenerContainer(AbstractMessageListenerContainer<K, V> container,
			ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties, TopicPartitionOffset... topicPartitions) {

		super(consumerFactory, containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.thisOrParentContainer = container == null ? this : container;
		if (topicPartitions != null) {
			this.topicPartitions = Arrays.copyOf(topicPartitions, topicPartitions.length);
		}
		else {
			this.topicPartitions = containerProperties.getTopicPartitionsToAssign();
		}
	}

	/**
	 * Set a {@link Runnable} to call whenever an {@link Error} occurs on a listener
	 * thread.
	 * @param emergencyStop the Runnable.
	 * @since 2.2.1
	 */
	public void setEmergencyStop(Runnable emergencyStop) {
		Assert.notNull(emergencyStop, "'emergencyStop' cannot be null");
		this.emergencyStop = emergencyStop;
	}

	/**
	 * Set a suffix to add to the {@code client.id} consumer property (if the consumer
	 * factory supports it).
	 * @param clientIdSuffix the suffix to add.
	 * @since 1.0.6
	 */
	public void setClientIdSuffix(String clientIdSuffix) {
		this.clientIdSuffix = clientIdSuffix;
	}

	/**
	 * Return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by Kafka; may be null if not assigned yet.
	 * @return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by Kafka; may be null if not assigned yet.
	 */
	@Override
	@Nullable
	public Collection<TopicPartition> getAssignedPartitions() {
		ListenerConsumer partitionsListenerConsumer = this.listenerConsumer;
		if (partitionsListenerConsumer != null) {
			if (partitionsListenerConsumer.definedPartitions != null) {
				return Collections.unmodifiableCollection(partitionsListenerConsumer.definedPartitions.keySet());
			}
			else if (partitionsListenerConsumer.assignedPartitions != null) {
				return Collections.unmodifiableCollection(partitionsListenerConsumer.assignedPartitions);
			}
			else {
				return null;
			}
		}
		else {
			return null;
		}
	}

	@Override
	public boolean isContainerPaused() {
		return isPaused() && this.listenerConsumer != null && this.listenerConsumer.isConsumerPaused();
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		ListenerConsumer listenerConsumerForMetrics = this.listenerConsumer;
		if (listenerConsumerForMetrics != null) {
			Map<MetricName, ? extends Metric> metrics = listenerConsumerForMetrics.consumer.metrics();
			Iterator<MetricName> metricIterator = metrics.keySet().iterator();
			if (metricIterator.hasNext()) {
				String clientId = metricIterator.next().tags().get("client-id");
				return Collections.singletonMap(clientId, metrics);
			}
		}
		return Collections.emptyMap();
	}

	@Override
	protected void doStart() {
		if (isRunning()) {
			return;
		}
		if (this.clientIdSuffix == null) { // stand-alone container
			checkTopics();
		}
		ContainerProperties containerProperties = getContainerProperties();
		checkAckMode(containerProperties);

		Object messageListener = containerProperties.getMessageListener();
		if (containerProperties.getConsumerTaskExecutor() == null) {
			SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}
		GenericMessageListener<?> listener = (GenericMessageListener<?>) messageListener;
		ListenerType listenerType = determineListenerType(listener);
		this.listenerConsumer = new ListenerConsumer(listener, listenerType);
		setRunning(true);
		this.startLatch = new CountDownLatch(1);
		this.listenerConsumerFuture = containerProperties
				.getConsumerTaskExecutor()
				.submitListenable(this.listenerConsumer);
		try {
			if (!this.startLatch.await(containerProperties.getConsumerStartTimout().toMillis(), TimeUnit.MILLISECONDS)) {
				this.logger.error("Consumer thread failed to start - does the configured task executor "
						+ "have enough threads to support all containers and concurrency?");
				publishConsumerFailedToStart();
			}
		}
		catch (@SuppressWarnings(UNUSED) InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void checkAckMode(ContainerProperties containerProperties) {
		if (!this.consumerFactory.isAutoCommit()) {
			AckMode ackMode = containerProperties.getAckMode();
			if (ackMode.equals(AckMode.COUNT) || ackMode.equals(AckMode.COUNT_TIME)) {
				Assert.state(containerProperties.getAckCount() > 0, "'ackCount' must be > 0");
			}
			if ((ackMode.equals(AckMode.TIME) || ackMode.equals(AckMode.COUNT_TIME))
					&& containerProperties.getAckTime() == 0) {
				containerProperties.setAckTime(DEFAULT_ACK_TIME);
			}
		}
	}

	private ListenerType determineListenerType(GenericMessageListener<?> listener) {
		ListenerType listenerType = ListenerUtils.determineListenerType(listener);
		if (listener instanceof DelegatingMessageListener) {
			Object delegating = listener;
			while (delegating instanceof DelegatingMessageListener) {
				delegating = ((DelegatingMessageListener<?>) delegating).getDelegate();
			}
			listenerType = ListenerUtils.determineListenerType(delegating);
		}
		return listenerType;
	}

	@Override
	protected void doStop(final Runnable callback) {
		if (isRunning()) {
			this.listenerConsumerFuture.addCallback(new StopCallback(callback));
			setRunning(false);
			this.listenerConsumer.wakeIfNecessary();
		}
	}

	private void publishIdleContainerEvent(long idleTime, Consumer<?, ?> consumer, boolean paused) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ListenerContainerIdleEvent(this,
					this.thisOrParentContainer, idleTime, getBeanName(), getAssignedPartitions(), consumer, paused));
		}
	}

	private void publishNonResponsiveConsumerEvent(long timeSinceLastPoll, Consumer<?, ?> consumer) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(
					new NonResponsiveConsumerEvent(this, this.thisOrParentContainer, timeSinceLastPoll,
							getBeanName(), getAssignedPartitions(), consumer));
		}
	}

	private void publishConsumerPausedEvent(Collection<TopicPartition> partitions) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerPausedEvent(this, this.thisOrParentContainer,
					Collections.unmodifiableCollection(partitions)));
		}
	}

	private void publishConsumerResumedEvent(Collection<TopicPartition> partitions) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerResumedEvent(this, this.thisOrParentContainer,
					Collections.unmodifiableCollection(partitions)));
		}
	}

	private void publishConsumerStoppingEvent(Consumer<?, ?> consumer) {
		try {
			if (getApplicationEventPublisher() != null) {
				getApplicationEventPublisher().publishEvent(
						new ConsumerStoppingEvent(this, this.thisOrParentContainer, consumer, getAssignedPartitions()));
			}
		}
		catch (Exception e) {
			this.logger.error(e, "Failed to publish consumer stopping event");
		}
	}

	private void publishConsumerStoppedEvent() {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerStoppedEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerStartingEvent() {
		this.startLatch.countDown();
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerStartingEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerStartedEvent() {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerStartedEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerFailedToStart() {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerFailedToStartEvent(this, this.thisOrParentContainer));
		}
	}

	@Override
	protected AbstractMessageListenerContainer<?, ?> parentOrThis() {
		return this.thisOrParentContainer;
	}

	@Override
	public String toString() {
		return "KafkaMessageListenerContainer [id=" + getBeanName()
				+ (this.clientIdSuffix != null ? ", clientIndex=" + this.clientIdSuffix : "")
				+ ", topicPartitions="
				+ (getAssignedPartitions() == null ? "none assigned" : getAssignedPartitions())
				+ "]";
	}


	private final class ListenerConsumer implements SchedulingAwareRunnable, ConsumerSeekCallback {

		private static final int SIXTY = 60;

		private static final String UNCHECKED = "unchecked";

		private static final String RAWTYPES = "rawtypes";

		private static final String RAW_TYPES = RAWTYPES;

		private final LogAccessor logger = new LogAccessor(LogFactory.getLog(ListenerConsumer.class)); // NOSONAR hide

		private final ContainerProperties containerProperties = getContainerProperties();

		private final OffsetCommitCallback commitCallback = this.containerProperties.getCommitCallback() != null
				? this.containerProperties.getCommitCallback()
				: new LoggingCommitCallback();

		private final Consumer<K, V> consumer;

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final GenericMessageListener<?> genericListener;

		private final ConsumerSeekAware consumerSeekAwareListener;

		private final MessageListener<K, V> listener;

		private final BatchMessageListener<K, V> batchListener;

		private final ListenerType listenerType;

		private final boolean isConsumerAwareListener;

		private final boolean isBatchListener;

		private final boolean wantsFullRecords;

		private final boolean autoCommit;

		private final boolean isManualAck = this.containerProperties.getAckMode().equals(AckMode.MANUAL);

		private final boolean isCountAck = this.containerProperties.getAckMode().equals(AckMode.COUNT)
				|| this.containerProperties.getAckMode().equals(AckMode.COUNT_TIME);

		private final boolean isTimeOnlyAck = this.containerProperties.getAckMode().equals(AckMode.TIME);

		private final boolean isManualImmediateAck =
				this.containerProperties.getAckMode().equals(AckMode.MANUAL_IMMEDIATE);

		private final boolean isAnyManualAck = this.isManualAck || this.isManualImmediateAck;

		private final boolean isRecordAck = this.containerProperties.getAckMode().equals(AckMode.RECORD);

		private final BlockingQueue<ConsumerRecord<K, V>> acks = new LinkedBlockingQueue<>();

		private final BlockingQueue<TopicPartitionOffset> seeks = new LinkedBlockingQueue<>();

		private final ErrorHandler errorHandler;

		private final BatchErrorHandler batchErrorHandler;

		private final PlatformTransactionManager transactionManager = this.containerProperties.getTransactionManager();

		@SuppressWarnings(RAW_TYPES)
		private final KafkaAwareTransactionManager kafkaTxManager =
				this.transactionManager instanceof KafkaAwareTransactionManager
						? ((KafkaAwareTransactionManager) this.transactionManager) : null;

		private final TransactionTemplate transactionTemplate;

		private final String consumerGroupId = getGroupId();

		private final TaskScheduler taskScheduler;

		private final ScheduledFuture<?> monitorTask;

		private final LogIfLevelEnabled commitLogger = new LogIfLevelEnabled(this.logger,
				this.containerProperties.getCommitLogLevel());

		private final Duration pollTimeout = Duration.ofMillis(this.containerProperties.getPollTimeout());

		private final boolean checkNullKeyForExceptions;

		private final boolean checkNullValueForExceptions;

		private final boolean syncCommits = this.containerProperties.isSyncCommits();

		private final Duration syncCommitTimeout;

		private final RecordInterceptor<K, V> recordInterceptor = !isInterceptBeforeTx()
				? getRecordInterceptor()
				: null;

		private final RecordInterceptor<K, V> earlyRecordInterceptor = isInterceptBeforeTx()
				? getRecordInterceptor()
				: null;

		private final ConsumerSeekCallback seekCallback = new InitialOrIdleSeekCallback();

		private final long maxPollInterval;

		private final MicrometerHolder micrometerHolder;

		private final AtomicBoolean polling = new AtomicBoolean();

		private final boolean subBatchPerPartition = this.containerProperties.isSubBatchPerPartition();

		private final Duration authorizationExceptionRetryInterval =
				this.containerProperties.getAuthorizationExceptionRetryInterval();

		private Map<TopicPartition, OffsetMetadata> definedPartitions;

		private int count;

		private long last = System.currentTimeMillis();

		private boolean fatalError;

		private boolean taskSchedulerExplicitlySet;

		private long lastReceive = System.currentTimeMillis();

		private long lastAlertAt = this.lastReceive;

		private long nackSleep = -1;

		private int nackIndex;

		private Iterator<TopicPartition> batchIterator;

		private ConsumerRecords<K, V> lastBatch;

		private volatile boolean consumerPaused;

		private volatile Collection<TopicPartition> assignedPartitions;

		private volatile Thread consumerThread;

		private volatile long lastPoll = System.currentTimeMillis();

		@SuppressWarnings(UNCHECKED)
		ListenerConsumer(GenericMessageListener<?> listener, ListenerType listenerType) {
			Properties consumerProperties = new Properties(this.containerProperties.getKafkaConsumerProperties());
			this.autoCommit = determineAutoCommit(consumerProperties);
			this.consumer =
					KafkaMessageListenerContainer.this.consumerFactory.createConsumer(
							this.consumerGroupId,
							this.containerProperties.getClientId(),
							KafkaMessageListenerContainer.this.clientIdSuffix,
							consumerProperties);

			this.transactionTemplate = determineTransactionTemplate();
			this.genericListener = listener;
			this.consumerSeekAwareListener = checkConsumerSeekAware(listener);
			subscribeOrAssignTopics(this.consumer);
			GenericErrorHandler<?> errHandler = KafkaMessageListenerContainer.this.getGenericErrorHandler();
			if (listener instanceof BatchMessageListener) {
				this.listener = null;
				this.batchListener = (BatchMessageListener<K, V>) listener;
				this.isBatchListener = true;
				this.wantsFullRecords = this.batchListener.wantsPollResult();
			}
			else if (listener instanceof MessageListener) {
				this.listener = (MessageListener<K, V>) listener;
				this.batchListener = null;
				this.isBatchListener = false;
				this.wantsFullRecords = false;
			}
			else {
				throw new IllegalArgumentException("Listener must be one of 'MessageListener', "
						+ "'BatchMessageListener', or the variants that are consumer aware and/or "
						+ "Acknowledging"
						+ " not " + listener.getClass().getName());
			}
			this.listenerType = listenerType;
			this.isConsumerAwareListener = listenerType.equals(ListenerType.ACKNOWLEDGING_CONSUMER_AWARE)
					|| listenerType.equals(ListenerType.CONSUMER_AWARE);
			if (this.isBatchListener) {
				validateErrorHandler(true);
				this.errorHandler = new LoggingErrorHandler();
				this.batchErrorHandler = determineBatchErrorHandler(errHandler);
			}
			else {
				validateErrorHandler(false);
				this.errorHandler = determineErrorHandler(errHandler);
				this.batchErrorHandler = new BatchLoggingErrorHandler();
			}
			Assert.state(!this.isBatchListener || !this.isRecordAck,
					"Cannot use AckMode.RECORD with a batch listener");
			if (this.containerProperties.getScheduler() != null) {
				this.taskScheduler = this.containerProperties.getScheduler();
				this.taskSchedulerExplicitlySet = true;
			}
			else {
				ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
				threadPoolTaskScheduler.initialize();
				this.taskScheduler = threadPoolTaskScheduler;
			}
			this.monitorTask = this.taskScheduler.scheduleAtFixedRate(this::checkConsumer,
					Duration.ofSeconds(this.containerProperties.getMonitorInterval()));
			if (this.containerProperties.isLogContainerConfig()) {
				this.logger.info(this.toString());
			}
			Map<String, Object> props = KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties();
			this.checkNullKeyForExceptions = checkDeserializer(findDeserializerClass(props, false));
			this.checkNullValueForExceptions = checkDeserializer(findDeserializerClass(props, true));
			this.syncCommitTimeout = determineSyncCommitTimeout();
			if (this.containerProperties.getSyncCommitTimeout() == null) {
				// update the property so we can use it directly from code elsewhere
				this.containerProperties.setSyncCommitTimeout(this.syncCommitTimeout);
				if (KafkaMessageListenerContainer.this.thisOrParentContainer != null) {
					KafkaMessageListenerContainer.this.thisOrParentContainer
							.getContainerProperties()
							.setSyncCommitTimeout(this.syncCommitTimeout);
				}
			}
			this.maxPollInterval = obtainMaxPollInterval(consumerProperties);
			this.micrometerHolder = obtainMicrometerHolder();
		}

		private long obtainMaxPollInterval(Properties consumerProperties) {
			Object timeout = consumerProperties.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
			if (timeout == null) {
				timeout = KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties()
						.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
			}

			if (timeout instanceof Duration) {
				return ((Duration) timeout).toMillis();
			}
			else if (timeout instanceof Number) {
				return ((Number) timeout).longValue();
			}
			else if (timeout instanceof String) {
				return Long.parseLong((String) timeout);
			}
			else {
				if (timeout != null) {
					Object timeoutToLog = timeout;
					this.logger.warn(() -> "Unexpected type: " + timeoutToLog.getClass().getName()
							+ " in property '"
							+ ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
							+ "'; defaulting to 30 seconds.");
				}
				return Duration.ofSeconds(SIXTY / 2).toMillis(); // Default 'max.poll.interval.ms' is 30 seconds
			}
		}

		@Nullable
		private ConsumerSeekAware checkConsumerSeekAware(GenericMessageListener<?> candidate) {
			return candidate instanceof ConsumerSeekAware ? (ConsumerSeekAware) candidate : null;
		}

		boolean isConsumerPaused() {
			return this.consumerPaused;
		}

		@Nullable
		private TransactionTemplate determineTransactionTemplate() {
			return this.transactionManager != null
					? new TransactionTemplate(this.transactionManager)
					: null;
		}

		private boolean determineAutoCommit(Properties consumerProperties) {
			boolean isAutoCommit;
			String autoCommitOverride = consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
			if (!KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties()
							.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
					&& autoCommitOverride == null) {
				consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
				isAutoCommit = false;
			}
			else if (autoCommitOverride != null) {
				isAutoCommit = Boolean.parseBoolean(autoCommitOverride);
			}
			else {
				isAutoCommit = KafkaMessageListenerContainer.this.consumerFactory.isAutoCommit();
			}
			Assert.state(!this.isAnyManualAck || !isAutoCommit,
					() -> "Consumer cannot be configured for auto commit for ackMode "
							+ this.containerProperties.getAckMode());
			return isAutoCommit;
		}

		private Duration determineSyncCommitTimeout() {
			if (this.containerProperties.getSyncCommitTimeout() != null) {
				return this.containerProperties.getSyncCommitTimeout();
			}
			else {
				Object timeout = this.containerProperties.getKafkaConsumerProperties()
						.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
				if (timeout == null) {
					timeout = KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties()
							.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
				}
				if (timeout instanceof Duration) {
					return (Duration) timeout;
				}
				else if (timeout instanceof Number) {
					return Duration.ofMillis(((Number) timeout).longValue());
				}
				else if (timeout instanceof String) {
					return Duration.ofMillis(Long.parseLong((String) timeout));
				}
				else {
					if (timeout != null) {
						Object timeoutToLog = timeout;
						this.logger.warn(() -> "Unexpected type: " + timeoutToLog.getClass().getName()
							+ " in property '"
							+ ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
							+ "'; defaulting to 60 seconds for sync commit timeouts");
					}
					return Duration.ofSeconds(SIXTY);
				}
			}

		}

		private Object findDeserializerClass(Map<String, Object> props, boolean isValue) {
			Object configuredDeserializer = isValue
					? KafkaMessageListenerContainer.this.consumerFactory.getValueDeserializer()
					: KafkaMessageListenerContainer.this.consumerFactory.getKeyDeserializer();
			if (configuredDeserializer == null) {
				return props.get(isValue
						? ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
						: ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
			}
			else {
				return configuredDeserializer.getClass();
			}
		}

		private void subscribeOrAssignTopics(final Consumer<? super K, ? super V> subscribingConsumer) {
			if (KafkaMessageListenerContainer.this.topicPartitions == null) {
				ConsumerRebalanceListener rebalanceListener = new ListenerConsumerRebalanceListener();
				Pattern topicPattern = this.containerProperties.getTopicPattern();
				if (topicPattern != null) {
					subscribingConsumer.subscribe(topicPattern, rebalanceListener);
				}
				else {
					subscribingConsumer.subscribe(Arrays.asList(this.containerProperties.getTopics()), // NOSONAR
							rebalanceListener);
				}
			}
			else {
				List<TopicPartitionOffset> topicPartitionsToAssign =
						Arrays.asList(KafkaMessageListenerContainer.this.topicPartitions);
				this.definedPartitions = new HashMap<>(topicPartitionsToAssign.size());
				for (TopicPartitionOffset topicPartition : topicPartitionsToAssign) {
					this.definedPartitions.put(topicPartition.getTopicPartition(),
							new OffsetMetadata(topicPartition.getOffset(), topicPartition.isRelativeToCurrent(),
									topicPartition.getPosition()));
				}
				subscribingConsumer.assign(new ArrayList<>(this.definedPartitions.keySet()));
			}
		}

		private boolean checkDeserializer(Object deser) {
			return deser instanceof Class
					? ErrorHandlingDeserializer2.class.isAssignableFrom((Class<?>) deser)
					: deser instanceof String && deser.equals(ErrorHandlingDeserializer2.class.getName());
		}

		protected void checkConsumer() {
			long timeSinceLastPoll = System.currentTimeMillis() - this.lastPoll;
			if (((float) timeSinceLastPoll) / (float) this.containerProperties.getPollTimeout()
					> this.containerProperties.getNoPollThreshold()) {
				publishNonResponsiveConsumerEvent(timeSinceLastPoll, this.consumer);
			}
		}

		protected BatchErrorHandler determineBatchErrorHandler(GenericErrorHandler<?> errHandler) {
			return errHandler != null ? (BatchErrorHandler) errHandler
					: this.transactionManager != null ? null : new BatchLoggingErrorHandler();
		}

		protected ErrorHandler determineErrorHandler(GenericErrorHandler<?> errHandler) {
			return errHandler != null ? (ErrorHandler) errHandler
					: this.transactionManager != null ? null : new LoggingErrorHandler();
		}

		@Nullable
		private MicrometerHolder obtainMicrometerHolder() {
			MicrometerHolder holder = null;
			try {
				if (MICROMETER_PRESENT && this.containerProperties.isMicrometerEnabled()) {
					holder = new MicrometerHolder(getApplicationContext(), getBeanName(),
							this.containerProperties.getMicrometerTags());
				}
			}
			catch (@SuppressWarnings(UNUSED) IllegalStateException ex) {
				// NOSONAR - no micrometer or meter registry
			}
			return holder;
		}

		private void seekPartitions(Collection<TopicPartition> partitions, boolean idle) {
			this.consumerSeekAwareListener.registerSeekCallback(this);
			Map<TopicPartition, Long> current = new HashMap<>();
			for (TopicPartition topicPartition : partitions) {
				current.put(topicPartition, ListenerConsumer.this.consumer.position(topicPartition));
			}
			if (idle) {
				this.consumerSeekAwareListener.onIdleContainer(current, this.seekCallback);
			}
			else {
				this.consumerSeekAwareListener.onPartitionsAssigned(current, this.seekCallback);
			}
		}

		private void validateErrorHandler(boolean batch) {
			GenericErrorHandler<?> errHandler = KafkaMessageListenerContainer.this.getGenericErrorHandler();
			if (errHandler == null) {
				return;
			}
			Class<?> clazz = errHandler.getClass();
			Assert.state(batch
						? BatchErrorHandler.class.isAssignableFrom(clazz)
						: ErrorHandler.class.isAssignableFrom(clazz),
					() -> "Error handler is not compatible with the message listener, expecting an instance of "
					+ (batch ? "BatchErrorHandler" : "ErrorHandler") + " not " + errHandler.getClass().getName());
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			publishConsumerStartingEvent();
			this.consumerThread = Thread.currentThread();
			if (this.consumerSeekAwareListener != null) {
				this.consumerSeekAwareListener.registerSeekCallback(this);
			}
			KafkaUtils.setConsumerGroupId(this.consumerGroupId);
			this.count = 0;
			this.last = System.currentTimeMillis();
			initAssignedPartitions();
			publishConsumerStartedEvent();
			while (isRunning()) {
				try {
					pollAndInvoke();
				}
				catch (@SuppressWarnings(UNUSED) WakeupException e) {
					// Ignore, we're stopping or applying immediate foreign acks
				}
				catch (NoOffsetForPartitionException nofpe) {
					this.fatalError = true;
					ListenerConsumer.this.logger.error(nofpe, "No offset and no reset policy");
					break;
				}
				catch (AuthorizationException ae) {
					if (this.authorizationExceptionRetryInterval == null) {
						ListenerConsumer.this.logger.error(ae, "Authorization Exception and no authorizationExceptionRetryInterval set");
						this.fatalError = true;
						break;
					}
					else {
						ListenerConsumer.this.logger.error(ae, "Authorization Exception, retrying in " + this.authorizationExceptionRetryInterval.toMillis() + " ms");
						// We can't pause/resume here, as KafkaConsumer doesn't take pausing
						// into account when committing, hence risk of being flooded with
						// GroupAuthorizationExceptions.
						// see: https://github.com/spring-projects/spring-kafka/pull/1337
						sleepFor(this.authorizationExceptionRetryInterval);
					}
				}
				catch (Exception e) {
					handleConsumerException(e);
				}
				catch (Error e) { // NOSONAR - rethrown
					Runnable runnable = KafkaMessageListenerContainer.this.emergencyStop;
					if (runnable != null) {
						runnable.run();
					}
					this.logger.error(e, "Stopping container due to an Error");
					wrapUp();
					throw e;
				}
			}
			wrapUp();
		}

		private void initAssignedPartitions() {
			if (isRunning() && this.definedPartitions != null) {
				try {
					initPartitionsIfNeeded();
				}
				catch (Exception e) {
					this.logger.error(e, "Failed to set initial offsets");
				}
			}
		}

		protected void pollAndInvoke() {
			if (!this.autoCommit && !this.isRecordAck) {
				processCommits();
			}
			idleBetweenPollIfNecessary();
			if (this.seeks.size() > 0) {
				processSeeks();
			}
			pauseConsumerIfNecessary();
			this.lastPoll = System.currentTimeMillis();
			this.polling.set(true);
			ConsumerRecords<K, V> records = doPoll();
			if (!this.polling.compareAndSet(true, false)) {
				/*
				 * There is a small race condition where wakeIfNecessary was called between
				 * exiting the poll and before we reset the boolean.
				 */
				if (records.count() > 0) {
					this.logger.debug(() -> "Discarding polled records, container stopped: " + records.count());
				}
				return;
			}
			resumeConsumerIfNeccessary();
			debugRecords(records);
			if (records != null && records.count() > 0) {
				if (this.containerProperties.getIdleEventInterval() != null) {
					this.lastReceive = System.currentTimeMillis();
				}
				invokeListener(records);
			}
			else {
				checkIdle();
			}
		}

		private ConsumerRecords<K, V> doPoll() {
			ConsumerRecords<K, V> records;
			if (this.isBatchListener && this.subBatchPerPartition) {
				if (this.batchIterator == null) {
					this.lastBatch = this.consumer.poll(this.pollTimeout);
					if (this.lastBatch.count() == 0) {
						return this.lastBatch;
					}
					else {
						this.batchIterator = this.lastBatch.partitions().iterator();
					}
				}
				TopicPartition next = this.batchIterator.next();
				List<ConsumerRecord<K, V>> subBatch = this.lastBatch.records(next);
				records = new ConsumerRecords<>(Collections.singletonMap(next, subBatch));
				if (!this.batchIterator.hasNext()) {
					this.batchIterator = null;
				}
			}
			else {
				records = this.consumer.poll(this.pollTimeout);
			}
			return records;
		}

		void wakeIfNecessary() {
			if (this.polling.getAndSet(false)) {
				this.consumer.wakeup();
			}
		}

		private void debugRecords(ConsumerRecords<K, V> records) {
			if (records != null) {
				this.logger.debug(() -> "Received: " + records.count() + " records");
				if (records.count() > 0) {
					this.logger.trace(() -> records.partitions().stream()
							.flatMap(p -> records.records(p).stream())
							// map to same format as send metadata toString()
							.map(r -> r.topic() + "-" + r.partition() + "@" + r.offset())
							.collect(Collectors.toList()).toString());
				}
			}
		}

		private void sleepFor(Duration duration) {
			try {
				TimeUnit.MILLISECONDS.sleep(duration.toMillis());
			}
			catch (InterruptedException e) {
				this.logger.error(e, "Interrupted while sleeping");
			}
		}

		private void pauseConsumerIfNecessary() {
			if (!this.consumerPaused && isPaused()) {
				this.consumer.pause(this.consumer.assignment());
				this.consumerPaused = true;
				this.logger.debug(() -> "Paused consumption from: " + this.consumer.paused());
				publishConsumerPausedEvent(this.consumer.assignment());
			}
		}

		private void resumeConsumerIfNeccessary() {
			if (this.consumerPaused && !isPaused()) {
				this.logger.debug(() -> "Resuming consumption from: " + this.consumer.paused());
				Set<TopicPartition> paused = this.consumer.paused();
				this.consumer.resume(paused);
				this.consumerPaused = false;
				publishConsumerResumedEvent(paused);
			}
		}

		private void checkIdle() {
			if (this.containerProperties.getIdleEventInterval() != null) {
				long now = System.currentTimeMillis();
				if (now > this.lastReceive + this.containerProperties.getIdleEventInterval()
						&& now > this.lastAlertAt + this.containerProperties.getIdleEventInterval()) {
					publishIdleContainerEvent(now - this.lastReceive, this.isConsumerAwareListener
							? this.consumer : null, this.consumerPaused);
					this.lastAlertAt = now;
					if (this.consumerSeekAwareListener != null) {
						Collection<TopicPartition> partitions = getAssignedPartitions();
						if (partitions != null) {
							seekPartitions(partitions, true);
						}
					}
				}
			}
		}

		private void idleBetweenPollIfNecessary() {
			long idleBetweenPolls = this.containerProperties.getIdleBetweenPolls();
			if (idleBetweenPolls > 0) {
				idleBetweenPolls = Math.min(idleBetweenPolls,
						this.maxPollInterval - (System.currentTimeMillis() - this.lastPoll)
								- 5000); // NOSONAR - less by five seconds to avoid race condition with rebalance
				if (idleBetweenPolls > 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(idleBetweenPolls);
					}
					catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						throw new IllegalStateException("Consumer Thread [" + this + "] has been interrupted", ex);
					}
				}
			}
		}

		private void wrapUp() {
			KafkaUtils.clearConsumerGroupId();
			if (this.micrometerHolder != null) {
				this.micrometerHolder.destroy();
			}
			publishConsumerStoppingEvent(this.consumer);
			Collection<TopicPartition> partitions = getAssignedPartitions();
			if (!this.fatalError) {
				if (this.kafkaTxManager == null) {
					commitPendingAcks();
					try {
						this.consumer.unsubscribe();
					}
					catch (@SuppressWarnings(UNUSED) WakeupException e) {
						// No-op. Continue process
					}
				}
				else {
					closeProducers(partitions);
				}
			}
			else {
				this.logger.error("Fatal consumer exception; stopping container");
				KafkaMessageListenerContainer.this.stop();
			}
			this.monitorTask.cancel(true);
			if (!this.taskSchedulerExplicitlySet) {
				((ThreadPoolTaskScheduler) this.taskScheduler).destroy();
			}
			this.consumer.close();
			getAfterRollbackProcessor().clearThreadState();
			if (this.errorHandler != null) {
				this.errorHandler.clearThreadState();
			}
			if (this.consumerSeekAwareListener != null) {
				this.consumerSeekAwareListener.onPartitionsRevoked(partitions);
				this.consumerSeekAwareListener.unregisterSeekCallback();
			}
			this.logger.info(() -> getGroupId() + ": Consumer stopped");
			publishConsumerStoppedEvent();
		}

		/**
		 * Handle exceptions thrown by the consumer outside of message listener
		 * invocation (e.g. commit exceptions).
		 * @param e the exception.
		 */
		protected void handleConsumerException(Exception e) {
			try {
				if (!this.isBatchListener && this.errorHandler != null) {
					this.errorHandler.handle(e, Collections.emptyList(), this.consumer,
							KafkaMessageListenerContainer.this.thisOrParentContainer);
				}
				else if (this.isBatchListener && this.batchErrorHandler != null) {
					this.batchErrorHandler.handle(e, new ConsumerRecords<K, V>(Collections.emptyMap()), this.consumer,
							KafkaMessageListenerContainer.this.thisOrParentContainer);
				}
				else {
					this.logger.error(e, "Consumer exception");
				}
			}
			catch (Exception ex) {
				this.logger.error(ex, "Consumer exception");
			}
		}

		private void commitPendingAcks() {
			processCommits();
			if (this.offsets.size() > 0) {
				// we always commit after stopping the invoker
				commitIfNecessary();
			}
		}

		/**
		 * Process any acks that have been queued.
		 */
		private void handleAcks() {
			ConsumerRecord<K, V> record = this.acks.poll();
			while (record != null) {
				traceAck(record);
				processAck(record);
				record = this.acks.poll();
			}
		}

		private void traceAck(ConsumerRecord<K, V> record) {
			this.logger.trace(() -> "Ack: " + record);
		}

		private void processAck(ConsumerRecord<K, V> record) {
			if (!Thread.currentThread().equals(this.consumerThread)) {
				try {
					this.acks.put(record);
					if (this.isManualImmediateAck) {
						this.consumer.wakeup();
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new KafkaException("Interrupted while storing ack", e);
				}
			}
			else {
				if (this.isManualImmediateAck) {
					try {
						ackImmediate(record);
					}
					catch (@SuppressWarnings(UNUSED) WakeupException e) {
						// ignore - not polling
					}
				}
				else {
					addOffset(record);
				}
			}
		}

		private void ackImmediate(ConsumerRecord<K, V> record) {
			Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
					new TopicPartition(record.topic(), record.partition()),
					new OffsetAndMetadata(record.offset() + 1));
			this.commitLogger.log(() -> "Committing: " + commits);
			if (this.syncCommits) {
				this.consumer.commitSync(commits, this.syncCommitTimeout);
			}
			else {
				this.consumer.commitAsync(commits, this.commitCallback);
			}
		}

		private void invokeListener(final ConsumerRecords<K, V> records) {
			if (this.isBatchListener) {
				invokeBatchListener(records);
			}
			else {
				invokeRecordListener(records);
			}
		}

		private void invokeBatchListener(final ConsumerRecords<K, V> records) {
			List<ConsumerRecord<K, V>> recordList = null;
			if (!this.wantsFullRecords) {
				recordList = createRecordList(records);
			}
			if (this.wantsFullRecords || recordList.size() > 0) {
				if (this.transactionTemplate != null) {
					invokeBatchListenerInTx(records, recordList);
				}
				else {
					doInvokeBatchListener(records, recordList, null);
				}
			}
		}

		@SuppressWarnings({ UNCHECKED, RAW_TYPES })
		private void invokeBatchListenerInTx(final ConsumerRecords<K, V> records,
				final List<ConsumerRecord<K, V>> recordList) {

			try {
				if (this.subBatchPerPartition) {
					ConsumerRecord<K, V> record = recordList.get(0);
					TransactionSupport.setTransactionIdSuffix(zombieFenceTxIdSuffix(record.topic(), record.partition()));
				}
				this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {

					@Override
					public void doInTransactionWithoutResult(TransactionStatus s) {
						Producer producer = null;
						if (ListenerConsumer.this.kafkaTxManager != null) {
							producer = ((KafkaResourceHolder) TransactionSynchronizationManager
									.getResource(ListenerConsumer.this.kafkaTxManager.getProducerFactory()))
										.getProducer(); // NOSONAR nullable
						}
						RuntimeException aborted = doInvokeBatchListener(records, recordList, producer);
						if (aborted != null) {
							throw aborted;
						}
					}
				});
			}
			catch (RuntimeException e) {
				this.logger.error(e, "Transaction rolled back");
				AfterRollbackProcessor<K, V> afterRollbackProcessorToUse =
						(AfterRollbackProcessor<K, V>) getAfterRollbackProcessor();
				if (afterRollbackProcessorToUse.isProcessInTransaction() && this.transactionTemplate != null) {
					this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {

						@Override
						protected void doInTransactionWithoutResult(TransactionStatus status) {
							batchAfterRollback(records, recordList, e, afterRollbackProcessorToUse);
						}

					});
				}
				else {
					batchAfterRollback(records, recordList, e, afterRollbackProcessorToUse);
				}
			}
			finally {
				if (this.subBatchPerPartition) {
					TransactionSupport.clearTransactionIdSuffix();
				}
			}
		}

		private void batchAfterRollback(final ConsumerRecords<K, V> records,
				final List<ConsumerRecord<K, V>> recordList, RuntimeException e,
				AfterRollbackProcessor<K, V> afterRollbackProcessorToUse) {

			try {
				if (recordList == null) {
					afterRollbackProcessorToUse.process(createRecordList(records), this.consumer, e, false);
				}
				else {
					afterRollbackProcessorToUse.process(recordList, this.consumer, e, false);
				}
			}
			catch (Exception ex) {
				this.logger.error(ex, "AfterRollbackProcessor threw exception");
			}
		}

		private List<ConsumerRecord<K, V>> createRecordList(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			List<ConsumerRecord<K, V>> list = new LinkedList<>();
			while (iterator.hasNext()) {
				list.add(iterator.next());
			}
			return list;
		}

		/**
		 * Actually invoke the batch listener.
		 * @param records the records (needed to invoke the error handler)
		 * @param recordList the list of records (actually passed to the listener).
		 * @param producer the producer - only if we're running in a transaction, null
		 * otherwise.
		 * @return an exception.
		 * @throws Error an error.
		 */
		private RuntimeException doInvokeBatchListener(final ConsumerRecords<K, V> records, // NOSONAR
				List<ConsumerRecord<K, V>> recordList, @SuppressWarnings(RAW_TYPES) Producer producer) {

			Object sample = startMicrometerSample();
			try {
				invokeBatchOnMessage(records, recordList, producer);
				successTimer(sample);
			}
			catch (RuntimeException e) {
				failureTimer(sample);
				boolean acked = this.containerProperties.isAckOnError() && !this.autoCommit && producer == null;
				if (acked) {
					this.acks.addAll(getHighestOffsetRecords(records));
				}
				if (this.batchErrorHandler == null) {
					throw e;
				}
				try {
					invokeBatchErrorHandler(records, e);
					// unlikely, but possible, that a batch error handler "handles" the error
					if ((!acked && !this.autoCommit && this.batchErrorHandler.isAckAfterHandle()) || producer != null) {
						this.acks.addAll(getHighestOffsetRecords(records));
						if (producer != null) {
							sendOffsetsToTransaction(producer);
						}
					}
				}
				catch (RuntimeException ee) {
					this.logger.error(ee, "Error handler threw an exception");
					return ee;
				}
				catch (Error er) { // NOSONAR
					this.logger.error(er, "Error handler threw an error");
					throw er;
				}
			}
			catch (@SuppressWarnings(UNUSED) InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return null;
		}

		@Nullable
		private Object startMicrometerSample() {
			if (this.micrometerHolder != null) {
				return this.micrometerHolder.start();
			}
			return null;
		}

		private void successTimer(@Nullable Object sample) {
			if (sample != null) {
				this.micrometerHolder.success(sample);
			}
		}

		private void failureTimer(@Nullable Object sample) {
			if (sample != null) {
				this.micrometerHolder.failure(sample);
			}
		}

		private void invokeBatchOnMessage(final ConsumerRecords<K, V> records, // NOSONAR - Cyclomatic Complexity
				List<ConsumerRecord<K, V>> recordList, @SuppressWarnings(RAW_TYPES) Producer producer) throws InterruptedException {

			if (this.wantsFullRecords) {
				this.batchListener.onMessage(records,
						this.isAnyManualAck
								? new ConsumerBatchAcknowledgment(records)
								: null,
						this.consumer);
			}
			else {
				doInvokeBatchOnMessage(records, recordList);
			}
			List<ConsumerRecord<?, ?>> toSeek = null;
			if (this.nackSleep >= 0) {
				int index = 0;
				toSeek = new ArrayList<>();
				for (ConsumerRecord<K, V> record : records) {
					if (index++ >= this.nackIndex) {
						toSeek.add(record);
					}
					else {
						this.acks.put(record);
					}
				}
			}
			if (!this.isAnyManualAck && !this.autoCommit) {
				for (ConsumerRecord<K, V> record : getHighestOffsetRecords(records)) {
					this.acks.put(record);
				}
				if (producer != null) {
					sendOffsetsToTransaction(producer);
				}
			}
			if (toSeek != null) {
				if (!this.autoCommit) {
					processCommits();
				}
				SeekUtils.doSeeks(toSeek, this.consumer, null, true, (rec, ex) -> false, this.logger);
				nackSleepAndReset();
			}
		}

		private void doInvokeBatchOnMessage(final ConsumerRecords<K, V> records,
				List<ConsumerRecord<K, V>> recordList) {

			switch (this.listenerType) {
				case ACKNOWLEDGING_CONSUMER_AWARE:
					this.batchListener.onMessage(recordList,
							this.isAnyManualAck
									? new ConsumerBatchAcknowledgment(records)
									: null, this.consumer);
					break;
				case ACKNOWLEDGING:
					this.batchListener.onMessage(recordList,
							this.isAnyManualAck
									? new ConsumerBatchAcknowledgment(records)
									: null);
					break;
				case CONSUMER_AWARE:
					this.batchListener.onMessage(recordList, this.consumer);
					break;
				case SIMPLE:
					this.batchListener.onMessage(recordList);
					break;
			}
		}

		private void invokeBatchErrorHandler(final ConsumerRecords<K, V> records, RuntimeException e) {
			if (this.batchErrorHandler instanceof ContainerAwareBatchErrorHandler) {
				this.batchErrorHandler.handle(decorateException(e), records, this.consumer,
						KafkaMessageListenerContainer.this.thisOrParentContainer);
			}
			else {
				this.batchErrorHandler.handle(decorateException(e), records, this.consumer);
			}
		}

		private void invokeRecordListener(final ConsumerRecords<K, V> records) {
			if (this.transactionTemplate != null) {
				invokeRecordListenerInTx(records);
			}
			else {
				doInvokeWithRecords(records);
			}
		}

		/**
		 * Invoke the listener with each record in a separate transaction.
		 * @param records the records.
		 */
		@SuppressWarnings(RAW_TYPES)
		private void invokeRecordListenerInTx(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext()) {
				final ConsumerRecord<K, V> record = checkEarlyIntercept(iterator.next());
				if (record == null) {
					continue;
				}
				this.logger.trace(() -> "Processing " + record);
				try {
					TransactionSupport
							.setTransactionIdSuffix(zombieFenceTxIdSuffix(record.topic(), record.partition()));
					this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {

						@Override
						public void doInTransactionWithoutResult(TransactionStatus s) {
							Producer producer = null;
							if (ListenerConsumer.this.kafkaTxManager != null) {
								producer = ((KafkaResourceHolder) TransactionSynchronizationManager
										.getResource(ListenerConsumer.this.kafkaTxManager.getProducerFactory()))
												.getProducer(); // NOSONAR
							}
							RuntimeException aborted = doInvokeRecordListener(record, producer, iterator);
							if (aborted != null) {
								throw aborted;
							}
						}

					});
				}
				catch (RuntimeException e) {
					this.logger.error(e, "Transaction rolled back");
					recordAfterRollback(iterator, record, e);
				}
				finally {
					TransactionSupport.clearTransactionIdSuffix();
				}
				if (this.nackSleep >= 0) {
					handleNack(records, record);
					break;
				}

			}
		}

		private void recordAfterRollback(Iterator<ConsumerRecord<K, V>> iterator, final ConsumerRecord<K, V> record,
				RuntimeException e) {

			List<ConsumerRecord<K, V>> unprocessed = new ArrayList<>();
			unprocessed.add(record);
			while (iterator.hasNext()) {
				unprocessed.add(iterator.next());
			}
			@SuppressWarnings(UNCHECKED)
			AfterRollbackProcessor<K, V> afterRollbackProcessorToUse =
					(AfterRollbackProcessor<K, V>) getAfterRollbackProcessor();
			if (afterRollbackProcessorToUse.isProcessInTransaction() && this.transactionTemplate != null) {
				this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {

					@Override
					protected void doInTransactionWithoutResult(TransactionStatus status) {
						afterRollbackProcessorToUse.process(unprocessed, ListenerConsumer.this.consumer, e, true);
					}

				});
			}
			else {
				try {
					afterRollbackProcessorToUse.process(unprocessed, this.consumer, e, true);
				}
				catch (Exception ex) {
					this.logger.error(ex, "AfterRollbackProcessor threw exception");
				}
			}
		}

		private void doInvokeWithRecords(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext()) {
				final ConsumerRecord<K, V> record = checkEarlyIntercept(iterator.next());
				if (record == null) {
					continue;
				}
				this.logger.trace(() -> "Processing " + record);
				doInvokeRecordListener(record, null, iterator);
				if (this.nackSleep >= 0) {
					handleNack(records, record);
					break;
				}
			}
		}

		private ConsumerRecord<K, V> checkEarlyIntercept(ConsumerRecord<K, V> nextArg) {
			ConsumerRecord<K, V> next = nextArg;
			if (this.earlyRecordInterceptor != null) {
				next = this.earlyRecordInterceptor.intercept(next);
				if (next == null && this.logger.isDebugEnabled()) {
					this.logger.debug("RecordInterceptor returned null, skipping: " + nextArg);
				}
			}
			return next;
		}

		private void handleNack(final ConsumerRecords<K, V> records, final ConsumerRecord<K, V> record) {
			if (!this.autoCommit && !this.isRecordAck) {
				processCommits();
			}
			List<ConsumerRecord<?, ?>> list = new ArrayList<>();
			Iterator<ConsumerRecord<K, V>> iterator2 = records.iterator();
			while (iterator2.hasNext()) {
				ConsumerRecord<K, V> next = iterator2.next();
				if (next.equals(record) || list.size() > 0) {
					list.add(next);
				}
			}
			SeekUtils.doSeeks(list, this.consumer, null, true, (rec, ex) -> false, this.logger);
			nackSleepAndReset();
		}

		private void nackSleepAndReset() {
			try {
				Thread.sleep(this.nackSleep);
			}
			catch (@SuppressWarnings(UNUSED) InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			this.nackSleep = -1;
		}

		/**
		 * Actually invoke the listener.
		 * @param record the record.
		 * @param producer the producer - only if we're running in a transaction, null
		 * otherwise.
		 * @param iterator the {@link ConsumerRecords} iterator - used only if a
		 * {@link RemainingRecordsErrorHandler} is being used.
		 * @return an exception.
		 * @throws Error an error.
		 */
		private RuntimeException doInvokeRecordListener(final ConsumerRecord<K, V> record, // NOSONAR
				@SuppressWarnings(RAW_TYPES) Producer producer, Iterator<ConsumerRecord<K, V>> iterator) {

			Object sample = startMicrometerSample();

			try {
				invokeOnMessage(record, producer);
				successTimer(sample);
			}
			catch (RuntimeException e) {
				failureTimer(sample);
				boolean acked = this.containerProperties.isAckOnError() && !this.autoCommit && producer == null;
				if (acked) {
					ackCurrent(record);
				}
				if (this.errorHandler == null) {
					throw e;
				}
				try {
					invokeErrorHandler(record, producer, iterator, e);
					if ((!acked && !this.autoCommit && this.errorHandler.isAckAfterHandle()) || producer != null) {
						ackCurrent(record, producer);
					}
				}
				catch (RuntimeException ee) {
					this.logger.error(ee, "Error handler threw an exception");
					return ee;
				}
				catch (Error er) { // NOSONAR
					this.logger.error(er, "Error handler threw an error");
					throw er;
				}
			}
			return null;
		}

		private void invokeOnMessage(final ConsumerRecord<K, V> record,
				@SuppressWarnings(RAWTYPES) @Nullable Producer producer) {

			if (record.value() instanceof DeserializationException) {
				throw (DeserializationException) record.value();
			}
			if (record.key() instanceof DeserializationException) {
				throw (DeserializationException) record.key();
			}
			if (record.value() == null && this.checkNullValueForExceptions) {
				checkDeser(record, ErrorHandlingDeserializer2.VALUE_DESERIALIZER_EXCEPTION_HEADER);
			}
			if (record.key() == null && this.checkNullKeyForExceptions) {
				checkDeser(record, ErrorHandlingDeserializer2.KEY_DESERIALIZER_EXCEPTION_HEADER);
			}
			doInvokeOnMessage(record);
			if (this.nackSleep < 0) {
				ackCurrent(record, producer);
			}
		}

		private void doInvokeOnMessage(final ConsumerRecord<K, V> recordArg) {
			ConsumerRecord<K, V> record = recordArg;
			if (this.recordInterceptor != null) {
				record = this.recordInterceptor.intercept(record);
			}
			if (record == null) {
				this.logger.debug(() -> "RecordInterceptor returned null, skipping: " + recordArg);
			}
			else {
				switch (this.listenerType) {
					case ACKNOWLEDGING_CONSUMER_AWARE:
						this.listener.onMessage(record,
								this.isAnyManualAck
										? new ConsumerAcknowledgment(record)
										: null, this.consumer);
						break;
					case CONSUMER_AWARE:
						this.listener.onMessage(record, this.consumer);
						break;
					case ACKNOWLEDGING:
						this.listener.onMessage(record,
								this.isAnyManualAck
										? new ConsumerAcknowledgment(record)
										: null);
						break;
					case SIMPLE:
						this.listener.onMessage(record);
						break;
				}
			}
		}

		private void invokeErrorHandler(final ConsumerRecord<K, V> record,
				@SuppressWarnings(RAWTYPES) @Nullable Producer producer,
				Iterator<ConsumerRecord<K, V>> iterator, RuntimeException e) {

			if (this.errorHandler instanceof RemainingRecordsErrorHandler) {
				if (producer == null) {
					processCommits();
				}
				List<ConsumerRecord<?, ?>> records = new ArrayList<>();
				records.add(record);
				while (iterator.hasNext()) {
					records.add(iterator.next());
				}
				this.errorHandler.handle(decorateException(e), records, this.consumer,
						KafkaMessageListenerContainer.this.thisOrParentContainer);
			}
			else {
				this.errorHandler.handle(decorateException(e), record, this.consumer);
			}
		}

		private Exception decorateException(RuntimeException e) {
			Exception toHandle = e;
			if (toHandle instanceof ListenerExecutionFailedException) {
				toHandle = new ListenerExecutionFailedException(toHandle.getMessage(), this.consumerGroupId,
						toHandle.getCause());
			}
			else {
				toHandle = new ListenerExecutionFailedException("Listener failed", this.consumerGroupId, toHandle);
			}
			return toHandle;
		}

		public void checkDeser(final ConsumerRecord<K, V> record, String headerName) {
			DeserializationException exception = ListenerUtils.getExceptionFromHeader(record, headerName, this.logger);
			if (exception != null) {
				throw exception;
			}
		}

		public void ackCurrent(final ConsumerRecord<K, V> record) {
			ackCurrent(record, null);
		}

		public void ackCurrent(final ConsumerRecord<K, V> record,
				@SuppressWarnings(RAW_TYPES) @Nullable Producer producer) {

			if (this.isRecordAck) {
				Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
						Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
								new OffsetAndMetadata(record.offset() + 1));
				if (producer == null) {
					this.commitLogger.log(() -> "Committing: " + offsetsToCommit);
					if (this.syncCommits) {
						this.consumer.commitSync(offsetsToCommit, this.syncCommitTimeout);
					}
					else {
						this.consumer.commitAsync(offsetsToCommit, this.commitCallback);
					}
				}
				else {
					this.acks.add(record);
				}
			}
			else if (!this.isAnyManualAck && !this.autoCommit) {
				this.acks.add(record);
			}
			if (producer != null) {
				try {
					sendOffsetsToTransaction(producer);
				}
				catch (Exception e) {
					this.logger.error(e, "Send offsets to transaction failed");
				}
			}
		}

		@SuppressWarnings({ UNCHECKED, RAW_TYPES })
		private void sendOffsetsToTransaction(Producer producer) {
			handleAcks();
			Map<TopicPartition, OffsetAndMetadata> commits = buildCommits();
			this.commitLogger.log(() -> "Sending offsets to transaction: " + commits);
			producer.sendOffsetsToTransaction(commits, this.consumerGroupId);
		}

		private void processCommits() {
			this.count += this.acks.size();
			handleAcks();
			AckMode ackMode = this.containerProperties.getAckMode();
			if (!this.isManualImmediateAck) {
				if (!this.isManualAck) {
					updatePendingOffsets();
				}
				boolean countExceeded = this.isCountAck && this.count >= this.containerProperties.getAckCount();
				if ((!this.isTimeOnlyAck && !this.isCountAck) || countExceeded) {
					if (this.isCountAck) {
						this.logger.debug(() -> "Committing in " + ackMode.name() + " because count "
								+ this.count
								+ " exceeds configured limit of " + this.containerProperties.getAckCount());
					}
					commitIfNecessary();
					this.count = 0;
				}
				else {
					timedAcks(ackMode);
				}
			}
		}

		private void timedAcks(AckMode ackMode) {
			long now;
			now = System.currentTimeMillis();
			boolean elapsed = now - this.last > this.containerProperties.getAckTime();
			if (ackMode.equals(AckMode.TIME) && elapsed) {
				this.logger.debug(() -> "Committing in AckMode.TIME " +
						"because time elapsed exceeds configured limit of " +
						this.containerProperties.getAckTime());
				commitIfNecessary();
				this.last = now;
			}
			else if (ackMode.equals(AckMode.COUNT_TIME) && elapsed) {
				this.logger.debug(() -> "Committing in AckMode.COUNT_TIME " +
						"because time elapsed exceeds configured limit of " +
						this.containerProperties.getAckTime());
				commitIfNecessary();
				this.last = now;
				this.count = 0;
			}
		}

		private void processSeeks() {
			processTimestampSeeks();
			TopicPartitionOffset offset = this.seeks.poll();
			while (offset != null) {
				traceSeek(offset);
				try {
					SeekPosition position = offset.getPosition();
					Long whereTo = offset.getOffset();
					if (position == null) {
						if (offset.isRelativeToCurrent()) {
							whereTo += this.consumer.position(offset.getTopicPartition());
							whereTo = Math.max(whereTo, 0);
						}
						this.consumer.seek(offset.getTopicPartition(), whereTo);
					}
					else if (position.equals(SeekPosition.BEGINNING)) {
						this.consumer.seekToBeginning(Collections.singletonList(offset.getTopicPartition()));
						if (whereTo != null) {
							this.consumer.seek(offset.getTopicPartition(), whereTo);
						}
					}
					else if (position.equals(SeekPosition.TIMESTAMP)) {
						// possible late addition since the grouped processing above
						Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer
								.offsetsForTimes(
										Collections.singletonMap(offset.getTopicPartition(), offset.getOffset()));
						offsetsForTimes.forEach((tp, ot) -> this.consumer.seek(tp, ot.offset()));
					}
					else {
						this.consumer.seekToEnd(Collections.singletonList(offset.getTopicPartition()));
						if (whereTo != null) {
							whereTo += this.consumer.position(offset.getTopicPartition());
							this.consumer.seek(offset.getTopicPartition(), whereTo);
						}
					}
				}
				catch (Exception e) {
					TopicPartitionOffset offsetToLog = offset;
					this.logger.error(e, () -> "Exception while seeking " + offsetToLog);
				}
				offset = this.seeks.poll();
			}
		}

		private void processTimestampSeeks() {
			Iterator<TopicPartitionOffset> seekIterator = this.seeks.iterator();
			Map<TopicPartition, Long> timestampSeeks = null;
			while (seekIterator.hasNext()) {
				TopicPartitionOffset tpo = seekIterator.next();
				if (SeekPosition.TIMESTAMP.equals(tpo.getPosition())) {
					if (timestampSeeks == null) {
						timestampSeeks = new HashMap<>();
					}
					timestampSeeks.put(tpo.getTopicPartition(), tpo.getOffset());
					seekIterator.remove();
					traceSeek(tpo);
				}
			}
			if (timestampSeeks != null) {
				Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer
						.offsetsForTimes(timestampSeeks);
				offsetsForTimes.forEach((tp, ot) -> this.consumer.seek(tp, ot.offset()));
			}
		}

		private void traceSeek(TopicPartitionOffset offset) {
			this.logger.trace(() -> "Seek: " + offset);
		}

		private void initPartitionsIfNeeded() {
			/*
			 * Note: initial position setting is only supported with explicit topic assignment.
			 * When using auto assignment (subscribe), the ConsumerRebalanceListener is not
			 * called until we poll() the consumer. Users can use a ConsumerAwareRebalanceListener
			 * or a ConsumerSeekAware listener in that case.
			 */
			Map<TopicPartition, OffsetMetadata> partitions = new HashMap<>(this.definedPartitions);
			Set<TopicPartition> beginnings = partitions.entrySet().stream()
					.filter(e -> SeekPosition.BEGINNING.equals(e.getValue().seekPosition))
					.map(Entry::getKey)
					.collect(Collectors.toSet());
			beginnings.forEach(partitions::remove);
			Set<TopicPartition> ends = partitions.entrySet().stream()
					.filter(e -> SeekPosition.END.equals(e.getValue().seekPosition))
					.map(Entry::getKey)
					.collect(Collectors.toSet());
			ends.forEach(partitions::remove);
			if (beginnings.size() > 0) {
				this.consumer.seekToBeginning(beginnings);
			}
			if (ends.size() > 0) {
				this.consumer.seekToEnd(ends);
			}
			for (Entry<TopicPartition, OffsetMetadata> entry : partitions.entrySet()) {
				TopicPartition topicPartition = entry.getKey();
				OffsetMetadata metadata = entry.getValue();
				Long offset = metadata.offset;
				if (offset != null) {
					long newOffset = offset;

					if (offset < 0) {
						if (!metadata.relativeToCurrent) {
							this.consumer.seekToEnd(Collections.singletonList(topicPartition));
						}
						newOffset = Math.max(0, this.consumer.position(topicPartition) + offset);
					}
					else if (metadata.relativeToCurrent) {
						newOffset = this.consumer.position(topicPartition) + offset;
					}

					try {
						this.consumer.seek(topicPartition, newOffset);
						logReset(topicPartition, newOffset);
					}
					catch (Exception e) {
						long newOffsetToLog = newOffset;
						this.logger.error(e, () -> "Failed to set initial offset for " + topicPartition
								+ " at " + newOffsetToLog + ". Position is " + this.consumer.position(topicPartition));
					}
				}
			}
		}

		private void logReset(TopicPartition topicPartition, long newOffset) {
			this.logger.debug(() -> "Reset " + topicPartition + " to offset " + newOffset);
		}

		private void updatePendingOffsets() {
			ConsumerRecord<K, V> record = this.acks.poll();
			while (record != null) {
				addOffset(record);
				record = this.acks.poll();
			}
		}

		private void addOffset(ConsumerRecord<K, V> record) {
			this.offsets.computeIfAbsent(record.topic(), v -> new ConcurrentHashMap<>())
					.compute(record.partition(), (k, v) -> v == null ? record.offset() : Math.max(v, record.offset()));
		}

		private void commitIfNecessary() {
			Map<TopicPartition, OffsetAndMetadata> commits = buildCommits();
			this.logger.debug(() -> "Commit list: " + commits);
			if (!commits.isEmpty()) {
				this.commitLogger.log(() -> "Committing: " + commits);
				try {
					if (this.syncCommits) {
						this.consumer.commitSync(commits, this.syncCommitTimeout);
					}
					else {
						this.consumer.commitAsync(commits, this.commitCallback);
					}
				}
				catch (@SuppressWarnings(UNUSED) WakeupException e) {
					// ignore - not polling
					this.logger.debug("Woken up during commit");
				}
			}
		}

		private Map<TopicPartition, OffsetAndMetadata> buildCommits() {
			Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
			for (Entry<String, Map<Integer, Long>> entry : this.offsets.entrySet()) {
				for (Entry<Integer, Long> offset : entry.getValue().entrySet()) {
					commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
							new OffsetAndMetadata(offset.getValue() + 1));
				}
			}
			this.offsets.clear();
			return commits;
		}

		private Collection<ConsumerRecord<K, V>> getHighestOffsetRecords(ConsumerRecords<K, V> records) {
			return records.partitions()
					.stream()
					.collect(Collectors.toMap(tp -> tp, tp -> {
						List<ConsumerRecord<K, V>> recordList = records.records(tp);
						return recordList.get(recordList.size() - 1);
					}))
					.values();
		}

		@Override
		public void seek(String topic, int partition, long offset) {
			this.seeks.add(new TopicPartitionOffset(topic, partition, offset));
		}

		@Override
		public void seekToBeginning(String topic, int partition) {
			this.seeks.add(new TopicPartitionOffset(topic, partition, SeekPosition.BEGINNING));
		}

		@Override
		public void seekToBeginning(Collection<TopicPartition> partitions) {
			this.seeks.addAll(partitions.stream()
					.map(tp -> new TopicPartitionOffset(tp.topic(), tp.partition(), SeekPosition.BEGINNING))
					.collect(Collectors.toList()));
		}

		@Override
		public void seekToEnd(String topic, int partition) {
			this.seeks.add(new TopicPartitionOffset(topic, partition, SeekPosition.END));
		}

		@Override
		public void seekToEnd(Collection<TopicPartition> partitions) {
			this.seeks.addAll(partitions.stream()
					.map(tp -> new TopicPartitionOffset(tp.topic(), tp.partition(), SeekPosition.END))
					.collect(Collectors.toList()));
		}

		@Override
		public void seekRelative(String topic, int partition, long offset, boolean toCurrent) {
			if (toCurrent) {
				this.seeks.add(new TopicPartitionOffset(topic, partition, offset, toCurrent));
			}
			else if (offset >= 0) {
				this.seeks.add(new TopicPartitionOffset(topic, partition, offset, SeekPosition.BEGINNING));
			}
			else {
				this.seeks.add(new TopicPartitionOffset(topic, partition, offset, SeekPosition.END));
			}
		}

		@Override
		public void seekToTimestamp(String topic, int partition, long timestamp) {
			this.seeks.add(new TopicPartitionOffset(topic, partition, timestamp, SeekPosition.TIMESTAMP));
		}

		@Override
		public void seekToTimestamp(Collection<TopicPartition> topicParts, long timestamp) {
			topicParts.forEach(tp -> seekToTimestamp(tp.topic(), tp.partition(), timestamp));
		}

		@Override
		public String toString() {
			return "KafkaMessageListenerContainer.ListenerConsumer ["
					+ "containerProperties=" + this.containerProperties
					+ ", listenerType=" + this.listenerType
					+ ", isConsumerAwareListener=" + this.isConsumerAwareListener
					+ ", isBatchListener=" + this.isBatchListener
					+ ", autoCommit=" + this.autoCommit
					+ ", consumerGroupId=" + this.consumerGroupId
					+ ", clientIdSuffix=" + KafkaMessageListenerContainer.this.clientIdSuffix
					+ "]";
		}

		private void closeProducers(@Nullable Collection<TopicPartition> partitions) {
			if (partitions != null) {
				ProducerFactory<?, ?> producerFactory = this.kafkaTxManager.getProducerFactory();
				partitions.forEach(tp -> {
					try {
						producerFactory.closeProducerFor(zombieFenceTxIdSuffix(tp.topic(), tp.partition()));
					}
					catch (Exception e) {
						this.logger.error(e, () -> "Failed to close producer with transaction id suffix: "
								+ zombieFenceTxIdSuffix(tp.topic(), tp.partition()));
					}
				});
			}
		}

		private String zombieFenceTxIdSuffix(String topic, int partition) {
			return this.consumerGroupId + "." + topic + "." + partition;
		}

		private final class ConsumerAcknowledgment implements Acknowledgment {

			private final ConsumerRecord<K, V> record;

			ConsumerAcknowledgment(ConsumerRecord<K, V> record) {
				this.record = record;
			}

			@Override
			public void acknowledge() {
				processAck(this.record);
			}

			@Override
			public void nack(long sleep) {
				Assert.state(Thread.currentThread().equals(ListenerConsumer.this.consumerThread),
						"nack() can only be called on the consumer thread");
				Assert.isTrue(sleep >= 0, "sleep cannot be negative");
				ListenerConsumer.this.nackSleep = sleep;
			}

			@Override
			public String toString() {
				return "Acknowledgment for " + this.record;
			}

		}

		private final class ConsumerBatchAcknowledgment implements Acknowledgment {

			private final ConsumerRecords<K, V> records;

			ConsumerBatchAcknowledgment(ConsumerRecords<K, V> records) {
				// make a copy in case the listener alters the list
				this.records = records;
			}

			@Override
			public void acknowledge() {
				for (ConsumerRecord<K, V> record : getHighestOffsetRecords(this.records)) {
					processAck(record);
				}
			}

			@Override
			public void nack(int index, long sleep) {
				Assert.state(Thread.currentThread().equals(ListenerConsumer.this.consumerThread),
						"nack() can only be called on the consumer thread");
				Assert.isTrue(sleep >= 0, "sleep cannot be negative");
				Assert.isTrue(index >= 0 && index < this.records.count(), "index out of bounds");
				ListenerConsumer.this.nackIndex = index;
				ListenerConsumer.this.nackSleep = sleep;
			}

			@Override
			public String toString() {
				return "Acknowledgment for " + this.records;
			}

		}

		private class ListenerConsumerRebalanceListener implements ConsumerRebalanceListener {

			private final ConsumerRebalanceListener userListener = getContainerProperties()
					.getConsumerRebalanceListener();

			private final ConsumerAwareRebalanceListener consumerAwareListener =
					this.userListener instanceof ConsumerAwareRebalanceListener
							? (ConsumerAwareRebalanceListener) this.userListener : null;

			ListenerConsumerRebalanceListener() {
			}

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				try {
					if (this.consumerAwareListener != null) {
						this.consumerAwareListener.onPartitionsRevokedBeforeCommit(ListenerConsumer.this.consumer,
								partitions);
					}
					else {
						this.userListener.onPartitionsRevoked(partitions);
					}
					// Wait until now to commit, in case the user listener added acks
					commitPendingAcks();
					if (this.consumerAwareListener != null) {
						this.consumerAwareListener.onPartitionsRevokedAfterCommit(ListenerConsumer.this.consumer,
								partitions);
					}
					if (ListenerConsumer.this.consumerSeekAwareListener != null) {
						ListenerConsumer.this.consumerSeekAwareListener.onPartitionsRevoked(partitions);
					}
					if (ListenerConsumer.this.assignedPartitions != null) {
						ListenerConsumer.this.assignedPartitions.removeAll(partitions);
					}
				}
				finally {
					if (ListenerConsumer.this.kafkaTxManager != null) {
						closeProducers(partitions);
					}
				}
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				if (ListenerConsumer.this.consumerPaused) {
					ListenerConsumer.this.consumer.pause(partitions);
					ListenerConsumer.this.logger.warn("Paused consumer resumed by Kafka due to rebalance; "
							+ "consumer paused again, so the initial poll() will never return any records");
				}
				ListenerConsumer.this.assignedPartitions = new LinkedList<>(partitions);
				if (!ListenerConsumer.this.autoCommit) {
					// Commit initial positions - this is generally redundant but
					// it protects us from the case when another consumer starts
					// and rebalance would cause it to reset at the end
					// see https://github.com/spring-projects/spring-kafka/issues/110
					Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
					for (TopicPartition partition : partitions) {
						try {
							offsetsToCommit.put(partition,
									new OffsetAndMetadata(ListenerConsumer.this.consumer.position(partition)));
						}
						catch (NoOffsetForPartitionException e) {
							ListenerConsumer.this.fatalError = true;
							ListenerConsumer.this.logger.error(e, "No offset and no reset policy");
							return;
						}
					}
					ListenerConsumer.this.commitLogger.log(() -> "Committing on assignment: " + offsetsToCommit);
					if (ListenerConsumer.this.transactionTemplate != null &&
							ListenerConsumer.this.kafkaTxManager != null) {
						try {
							offsetsToCommit.forEach((partition, offsetAndMetadata) -> {
								TransactionSupport.setTransactionIdSuffix(
										zombieFenceTxIdSuffix(partition.topic(), partition.partition()));
								ListenerConsumer.this.transactionTemplate
										.execute(new TransactionCallbackWithoutResult() {

											@SuppressWarnings({ UNCHECKED, RAWTYPES })
											@Override
											protected void doInTransactionWithoutResult(TransactionStatus status) {
												KafkaResourceHolder holder =
														(KafkaResourceHolder) TransactionSynchronizationManager
																.getResource(
																		ListenerConsumer.this.kafkaTxManager
																				.getProducerFactory());
												if (holder != null) {
													holder.getProducer()
															.sendOffsetsToTransaction(
																	Collections.singletonMap(partition,
																			offsetAndMetadata),
																	ListenerConsumer.this.consumerGroupId);
												}
											}

										});
							});
						}
						finally {
							TransactionSupport.clearTransactionIdSuffix();
						}
					}
					else {
						ContainerProperties containerProps = KafkaMessageListenerContainer.this.getContainerProperties();
						if (containerProps.isSyncCommits()) {
							ListenerConsumer.this.consumer.commitSync(offsetsToCommit,
									containerProps.getSyncCommitTimeout());
						}
						else {
							ListenerConsumer.this.consumer.commitAsync(offsetsToCommit,
									containerProps.getCommitCallback());
						}
					}
				}
				if (ListenerConsumer.this.genericListener instanceof ConsumerSeekAware) {
					seekPartitions(partitions, false);
				}
				if (this.consumerAwareListener != null) {
					this.consumerAwareListener.onPartitionsAssigned(ListenerConsumer.this.consumer, partitions);
				}
				else {
					this.userListener.onPartitionsAssigned(partitions);
				}
			}

			@Override
			public void onPartitionsLost(Collection<TopicPartition> partitions) {
				if (this.consumerAwareListener != null) {
					this.consumerAwareListener.onPartitionsLost(ListenerConsumer.this.consumer, partitions);
				}
				else {
					this.userListener.onPartitionsLost(partitions);
				}
				onPartitionsRevoked(partitions);
			}

		}

		private final class InitialOrIdleSeekCallback implements ConsumerSeekCallback {

			InitialOrIdleSeekCallback() {
			}

			@Override
			public void seek(String topic, int partition, long offset) {
				ListenerConsumer.this.consumer.seek(new TopicPartition(topic, partition), offset);
			}

			@Override
			public void seekToBeginning(String topic, int partition) {
				ListenerConsumer.this.consumer.seekToBeginning(
						Collections.singletonList(new TopicPartition(topic, partition)));
			}

			@Override
			public void seekToBeginning(Collection<TopicPartition> partitions) {
				ListenerConsumer.this.consumer.seekToBeginning(partitions);
			}

			@Override
			public void seekToEnd(String topic, int partition) {
				ListenerConsumer.this.consumer.seekToEnd(
						Collections.singletonList(new TopicPartition(topic, partition)));
			}

			@Override
			public void seekToEnd(Collection<TopicPartition> partitions) {
				ListenerConsumer.this.consumer.seekToEnd(partitions);
			}

			@Override
			public void seekRelative(String topic, int partition, long offset, boolean toCurrent) {
				TopicPartition topicPart = new TopicPartition(topic, partition);
				Long whereTo = null;
				Consumer<K, V> consumerToSeek = ListenerConsumer.this.consumer;
				if (offset >= 0) {
					whereTo = computeForwardWhereTo(offset, toCurrent, topicPart, consumerToSeek);
				}
				else {
					whereTo = computeBackwardWhereTo(offset, toCurrent, topicPart, consumerToSeek);
				}
				if (whereTo != null) {
					consumerToSeek.seek(topicPart, whereTo);
				}
			}

			@Override
			public void seekToTimestamp(String topic, int partition, long timestamp) {
				Consumer<K, V> consumerToSeek = ListenerConsumer.this.consumer;
				Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumerToSeek.offsetsForTimes(
						Collections.singletonMap(new TopicPartition(topic, partition), timestamp));
				offsetsForTimes.forEach((tp, ot) -> {
					if (ot != null) {
						consumerToSeek.seek(tp, ot.offset());
					}
				});
			}

			@Override
			public void seekToTimestamp(Collection<TopicPartition> topicParts, long timestamp) {
				Consumer<K, V> consumerToSeek = ListenerConsumer.this.consumer;
				Map<TopicPartition, Long> map = topicParts.stream()
						.collect(Collectors.toMap(tp -> tp, tp -> timestamp));
				Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumerToSeek.offsetsForTimes(map);
				offsetsForTimes.forEach((tp, ot) -> {
					if (ot != null) {
						consumerToSeek.seek(tp, ot.offset());
					}
				});
			}

			@Nullable
			private Long computeForwardWhereTo(long offset, boolean toCurrent, TopicPartition topicPart,
					Consumer<K, V> consumerToSeek) {

				Long start;
				if (!toCurrent) {
					Map<TopicPartition, Long> beginning = consumerToSeek
							.beginningOffsets(Collections.singletonList(topicPart));
					start = beginning.get(topicPart);
				}
				else {
					start = consumerToSeek.position(topicPart);
				}
				if (start != null) {
					return start + offset;
				}
				return null;
			}

			@Nullable
			private Long computeBackwardWhereTo(long offset, boolean toCurrent, TopicPartition topicPart,
					Consumer<K, V> consumerToSeek) {

				Long end;
				if (!toCurrent) {
					Map<TopicPartition, Long> endings = consumerToSeek
							.endOffsets(Collections.singletonList(topicPart));
					end = endings.get(topicPart);
				}
				else {
					end = consumerToSeek.position(topicPart);
				}
				if (end != null) {
					long newOffset = end + offset;
					return newOffset < 0 ? 0 : newOffset;
				}
				return null;
			}

		}

	}

	private static final class OffsetMetadata {

		private final Long offset;

		private final boolean relativeToCurrent;

		private final SeekPosition seekPosition;

		OffsetMetadata(Long offset, boolean relativeToCurrent, SeekPosition seekPosition) {
			this.offset = offset;
			this.relativeToCurrent = relativeToCurrent;
			this.seekPosition = seekPosition;
		}

	}

	private class StopCallback implements ListenableFutureCallback<Object> {

		private final Runnable callback;

		StopCallback(Runnable callback) {
			this.callback = callback;
		}

		@Override
		public void onFailure(Throwable e) {
			KafkaMessageListenerContainer.this.logger
					.error(e, "Error while stopping the container: ");
			if (this.callback != null) {
				this.callback.run();
			}
		}

		@Override
		public void onSuccess(Object result) {
			KafkaMessageListenerContainer.this.logger
					.debug(() -> KafkaMessageListenerContainer.this + " stopped normally");
			if (this.callback != null) {
				this.callback.run();
			}
		}

	}

	private static final class MicrometerHolder {

		private final Set<Timer> meters = ConcurrentHashMap.newKeySet();

		private final MeterRegistry registry;

		private final Timer successTimer;

		private final Timer failTimer;

		MicrometerHolder(@Nullable ApplicationContext context, String name, Map<String, String> tags) {
			if (context == null) {
				throw new IllegalStateException("No micrometer registry present");
			}
			Map<String, MeterRegistry> registries = context.getBeansOfType(MeterRegistry.class, false, false);
			if (registries.size() == 1) {
				this.registry = registries.values().iterator().next();
				this.successTimer = buildTimer(true, name, "none", tags);
				this.failTimer = buildTimer(false, name, "ListenerExecutionFailedException", tags);
			}
			else {
				throw new IllegalStateException("No micrometer registry present");
			}
		}

		Object start() {
			return Timer.start(this.registry);
		}

		void success(Object sample) {
			((Sample) sample).stop(this.successTimer);
		}

		void failure(Object sample) {
			((Sample) sample).stop(this.failTimer);
		}

		private Timer buildTimer(boolean result, String name, String exception, Map<String, String> tags) {
			Builder builder = Timer.builder("spring.kafka.listener")
				.description("Kafka Listener Timer")
				.tag("name", name)
				.tag("result", result ? "success" : "failure")
				.tag("exception", exception);
			if (tags != null && !tags.isEmpty()) {
				tags.entrySet().forEach(entry -> builder.tag(entry.getKey(), entry.getValue()));
			}
			Timer registeredTimer = builder.register(this.registry);
			this.meters.add(registeredTimer);
			return registeredTimer;
		}

		void destroy() {
			this.meters.forEach(this.registry::remove);
			this.meters.clear();
		}

	}

}
