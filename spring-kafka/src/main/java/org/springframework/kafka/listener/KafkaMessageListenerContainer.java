/*
 * Copyright 2016-2021 the original author or authors.
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerPartitionPausedEvent;
import org.springframework.kafka.event.ConsumerPartitionResumedEvent;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent.Reason;
import org.springframework.kafka.event.ConsumerStoppingEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.ListenerContainerNoLongerIdleEvent;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.event.ListenerContainerPartitionNoLongerIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ContainerProperties.AssignmentCommitOption;
import org.springframework.kafka.listener.ContainerProperties.EOSMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.support.micrometer.MicrometerHolder;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


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
 * @author Tomaz Fernandes
 */
public class KafkaMessageListenerContainer<K, V> // NOSONAR line count
		extends AbstractMessageListenerContainer<K, V> {

	private static final String UNUSED = "unused";

	private static final int DEFAULT_ACK_TIME = 5000;

	private static final Map<String, Object> CONSUMER_CONFIG_DEFAULTS = ConsumerConfig.configDef().defaultValues();

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
	 */
	KafkaMessageListenerContainer(@Nullable AbstractMessageListenerContainer<K, V> container,
			ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties, @Nullable TopicPartitionOffset... topicPartitions) {

		super(consumerFactory, containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.thisOrParentContainer = container == null ? this : container;
		if (topicPartitions != null) {
			this.topicPartitions = Arrays.copyOf(topicPartitions, topicPartitions.length);
		}
		else {
			this.topicPartitions = containerProperties.getTopicPartitions();
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
	@Nullable
	public Map<String, Collection<TopicPartition>> getAssignmentsByClientId() {
		ListenerConsumer partitionsListenerConsumer = this.listenerConsumer;
		if (this.listenerConsumer != null) {
			return Collections.singletonMap(partitionsListenerConsumer.getClientId(), getAssignedPartitions());
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
	public boolean isPartitionPaused(TopicPartition topicPartition) {
		return this.listenerConsumer != null && this.listenerConsumer
				.isPartitionPaused(topicPartition);
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		ListenerConsumer listenerConsumerForMetrics = this.listenerConsumer;
		if (listenerConsumerForMetrics != null) {
			Map<MetricName, ? extends Metric> metrics = listenerConsumerForMetrics.consumer.metrics();
			return Collections.singletonMap(listenerConsumerForMetrics.getClientId(), metrics);
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
		AsyncListenableTaskExecutor consumerExecutor = containerProperties.getConsumerTaskExecutor();
		if (consumerExecutor == null) {
			consumerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}
		GenericMessageListener<?> listener = (GenericMessageListener<?>) messageListener;
		ListenerType listenerType = determineListenerType(listener);
		this.listenerConsumer = new ListenerConsumer(listener, listenerType);
		setRunning(true);
		this.startLatch = new CountDownLatch(1);
		this.listenerConsumerFuture = consumerExecutor
				.submitListenable(this.listenerConsumer);
		try {
			if (!this.startLatch.await(containerProperties.getConsumerStartTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
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

	private void publishIdlePartitionEvent(long idleTime, TopicPartition topicPartition, Consumer<K, V> consumer, boolean paused) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ListenerContainerPartitionIdleEvent(this,
					this.thisOrParentContainer, idleTime, getBeanName(), topicPartition, consumer, paused));
		}
	}

	private void publishNoLongerIdlePartitionEvent(long idleTime, Consumer<K, V> consumer, TopicPartition topicPartition) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ListenerContainerPartitionNoLongerIdleEvent(this,
					this.thisOrParentContainer, idleTime, getBeanName(), topicPartition, consumer));
		}
	}

	private void publishIdleContainerEvent(long idleTime, Consumer<?, ?> consumer, boolean paused) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ListenerContainerIdleEvent(this,
					this.thisOrParentContainer, idleTime, getBeanName(), getAssignedPartitions(), consumer, paused));
		}
	}

	private void publishNoLongerIdleContainerEvent(long idleTime, Consumer<?, ?> consumer) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ListenerContainerNoLongerIdleEvent(this,
					this.thisOrParentContainer, idleTime, getBeanName(), getAssignedPartitions(), consumer));
		}
	}

	private void publishNonResponsiveConsumerEvent(long timeSinceLastPoll, Consumer<?, ?> consumer) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(
					new NonResponsiveConsumerEvent(this, this.thisOrParentContainer, timeSinceLastPoll,
							getBeanName(), getAssignedPartitions(), consumer));
		}
	}

	private void publishConsumerPausedEvent(Collection<TopicPartition> partitions) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerPausedEvent(this, this.thisOrParentContainer,
					Collections.unmodifiableCollection(partitions)));
		}
	}

	private void publishConsumerResumedEvent(Collection<TopicPartition> partitions) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerResumedEvent(this, this.thisOrParentContainer,
					Collections.unmodifiableCollection(partitions)));
		}
	}

	private void publishConsumerPartitionPausedEvent(TopicPartition partition) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerPartitionPausedEvent(this, this.thisOrParentContainer,
					partition));
		}
	}

	private void publishConsumerPartitionResumedEvent(TopicPartition partition) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerPartitionResumedEvent(this, this.thisOrParentContainer,
					partition));
		}
	}

	private void publishConsumerStoppingEvent(Consumer<?, ?> consumer) {
		try {
			ApplicationEventPublisher publisher = getApplicationEventPublisher();
			if (publisher != null) {
				publisher.publishEvent(
						new ConsumerStoppingEvent(this, this.thisOrParentContainer, consumer, getAssignedPartitions()));
			}
		}
		catch (Exception e) {
			this.logger.error(e, "Failed to publish consumer stopping event");
		}
	}

	private void publishConsumerStoppedEvent(@Nullable Throwable throwable) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			Reason reason;
			if (throwable instanceof Error) {
				reason = Reason.ERROR;
			}
			else if (throwable instanceof StopAfterFenceException || throwable instanceof FencedInstanceIdException) {
				reason = Reason.FENCED;
			}
			else if (throwable instanceof AuthorizationException) {
				reason = Reason.AUTH;
			}
			else if (throwable instanceof NoOffsetForPartitionException) {
				reason = Reason.NO_OFFSET;
			}
			else {
				reason = Reason.NORMAL;
			}
			publisher.publishEvent(new ConsumerStoppedEvent(this, this.thisOrParentContainer,
					reason));
		}
	}

	private void publishConsumerStartingEvent() {
		this.startLatch.countDown();
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerStartingEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerStartedEvent() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerStartedEvent(this, this.thisOrParentContainer));
		}
	}

	private void publishConsumerFailedToStart() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerFailedToStartEvent(this, this.thisOrParentContainer));
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

		private static final String ERROR_HANDLER_THREW_AN_EXCEPTION = "Error handler threw an exception";

		private static final String UNCHECKED = "unchecked";

		private static final String RAWTYPES = "rawtypes";

		private static final String RAW_TYPES = RAWTYPES;

		private final LogAccessor logger = KafkaMessageListenerContainer.this.logger; // NOSONAR hide

		private final ContainerProperties containerProperties = getContainerProperties();

		private final OffsetCommitCallback commitCallback = this.containerProperties.getCommitCallback() != null
				? this.containerProperties.getCommitCallback()
				: new LoggingCommitCallback();

		private final Consumer<K, V> consumer;

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final Collection<TopicPartition> assignedPartitions = new LinkedHashSet<>();

		private final Map<TopicPartition, OffsetAndMetadata> lastCommits = new HashMap<>();

		private final Map<TopicPartition, Long> savedPositions = new HashMap<>();

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

		private final CommonErrorHandler commonErrorHandler;

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

		private final RecordInterceptor<K, V> recordInterceptor = !isInterceptBeforeTx() && this.kafkaTxManager != null
				? getRecordInterceptor()
				: null;

		private final RecordInterceptor<K, V> earlyRecordInterceptor =
				isInterceptBeforeTx() || this.kafkaTxManager == null
						? getRecordInterceptor()
						: null;

		private final RecordInterceptor<K, V> commonRecordInterceptor = getRecordInterceptor();

		private final BatchInterceptor<K, V> batchInterceptor = !isInterceptBeforeTx() && this.kafkaTxManager != null
				? getBatchInterceptor()
				: null;

		private final BatchInterceptor<K, V> earlyBatchInterceptor =
				isInterceptBeforeTx() || this.kafkaTxManager == null
						? getBatchInterceptor()
						: null;

		private final BatchInterceptor<K, V> commonBatchInterceptor = getBatchInterceptor();

		private final ConsumerSeekCallback seekCallback = new InitialOrIdleSeekCallback();

		private final long maxPollInterval;

		private final MicrometerHolder micrometerHolder;

		private final AtomicBoolean polling = new AtomicBoolean();

		private final boolean subBatchPerPartition;

		private final Duration authorizationExceptionRetryInterval =
				this.containerProperties.getAuthorizationExceptionRetryInterval();

		private final AssignmentCommitOption autoCommitOption = this.containerProperties.getAssignmentCommitOption();

		private final boolean commitCurrentOnAssignment;

		private final DeliveryAttemptAware deliveryAttemptAware;

		private final EOSMode eosMode = this.containerProperties.getEosMode();

		private final Map<TopicPartition, OffsetAndMetadata> commitsDuringRebalance = new HashMap<>();

		private final String clientId;

		private final boolean fixTxOffsets = this.containerProperties.isFixTxOffsets();

		private final boolean stopImmediate = this.containerProperties.isStopImmediate();

		private final Set<TopicPartition> pausedPartitions;

		private final Map<TopicPartition, List<Long>> offsetsInThisBatch =
				this.isAnyManualAck && this.containerProperties.isAsyncAcks()
						? new HashMap<>()
						: null;

		private final Map<TopicPartition, List<ConsumerRecord<K, V>>> deferredOffsets =
				this.isAnyManualAck && this.containerProperties.isAsyncAcks()
						? new HashMap<>()
						: null;

		private final Map<TopicPartition, Long> lastReceivePartition;

		private final Map<TopicPartition, Long> lastAlertPartition;

		private final Map<TopicPartition, Boolean> wasIdlePartition;

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

		private Producer<?, ?> producer;

		private boolean producerPerConsumerPartition;

		private boolean commitRecovered;

		private boolean wasIdle;

		private boolean batchFailed;

		private boolean pausedForAsyncAcks;

		private boolean receivedSome;

		private volatile boolean consumerPaused;

		private volatile Thread consumerThread;

		private volatile long lastPoll = System.currentTimeMillis();

		@SuppressWarnings(UNCHECKED)
		ListenerConsumer(GenericMessageListener<?> listener, ListenerType listenerType) {
			Properties consumerProperties = propertiesFromProperties();
			checkGroupInstance(consumerProperties, KafkaMessageListenerContainer.this.consumerFactory);
			this.autoCommit = determineAutoCommit(consumerProperties);
			this.consumer =
					KafkaMessageListenerContainer.this.consumerFactory.createConsumer(
							this.consumerGroupId,
							this.containerProperties.getClientId(),
							KafkaMessageListenerContainer.this.clientIdSuffix,
							consumerProperties);

			this.clientId = determineClientId();
			this.transactionTemplate = determineTransactionTemplate();
			this.genericListener = listener;
			this.consumerSeekAwareListener = checkConsumerSeekAware(listener);
			this.commitCurrentOnAssignment = determineCommitCurrent(consumerProperties,
					KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties());
			subscribeOrAssignTopics(this.consumer);
			GenericErrorHandler<?> errHandler = getGenericErrorHandler();
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
			this.commonErrorHandler = determineCommonErrorHandler(errHandler);
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
			this.monitorTask = this.taskScheduler.scheduleAtFixedRate(this::checkConsumer, // NOSONAR
					Duration.ofSeconds(this.containerProperties.getMonitorInterval()));
			if (this.containerProperties.isLogContainerConfig()) {
				this.logger.info(toString());
			}
			Map<String, Object> props = KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties();
			this.checkNullKeyForExceptions = checkDeserializer(findDeserializerClass(props, consumerProperties, false));
			this.checkNullValueForExceptions = checkDeserializer(findDeserializerClass(props, consumerProperties, true));
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
			this.deliveryAttemptAware = setupDeliveryAttemptAware();
			this.subBatchPerPartition = setupSubBatchPerPartition();
			this.lastReceivePartition = new HashMap<>();
			this.lastAlertPartition = new HashMap<>();
			this.wasIdlePartition = new HashMap<>();
			this.pausedPartitions = new HashSet<>();
		}

		@Nullable
		private CommonErrorHandler determineCommonErrorHandler(@Nullable GenericErrorHandler<?> errHandler) {
			CommonErrorHandler common = getCommonErrorHandler();
			if (common != null) {
				if (errHandler != null) {
					this.logger.debug("GenericErrorHandler is ignored when a CommonErrorHandler is provided");
				}
				return common;
			}
			if (errHandler == null && this.transactionManager == null) {
				return new DefaultErrorHandler();
			}
			if (this.isBatchListener) {
				validateErrorHandler(true);
				BatchErrorHandler batchErrorHandler = (BatchErrorHandler) errHandler;
				if (batchErrorHandler != null) {
					return new ErrorHandlerAdapter(batchErrorHandler);
				}
				else {
					return null;
				}
			}
			else {
				validateErrorHandler(false);
				ErrorHandler eh = (ErrorHandler) errHandler;
				if (eh != null) {
					return new ErrorHandlerAdapter(eh);
				}
				else {
					return null;
				}
			}
		}

		private Properties propertiesFromProperties() {
			Properties propertyOverrides = this.containerProperties.getKafkaConsumerProperties();
			Properties props = new Properties();
			props.putAll(propertyOverrides);
			Set<String> stringPropertyNames = propertyOverrides.stringPropertyNames();
			// User might have provided properties as defaults
			stringPropertyNames.forEach((name) -> {
				if (!props.contains(name)) {
					props.setProperty(name, propertyOverrides.getProperty(name));
				}
			});
			return props;
		}

		String getClientId() {
			return this.clientId;
		}

		private String determineClientId() {
			Map<MetricName, ? extends Metric> metrics = this.consumer.metrics();
			Iterator<MetricName> metricIterator = metrics.keySet().iterator();
			if (metricIterator.hasNext()) {
				return metricIterator.next().tags().get("client-id");
			}
			return "unknown.client.id";
		}

		private void checkGroupInstance(Properties properties, ConsumerFactory<K, V> consumerFactory) {
			String groupInstance = properties.getProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
			if (!StringUtils.hasText(groupInstance)) {
				Object factoryConfig = consumerFactory.getConfigurationProperties()
						.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
				if (factoryConfig instanceof String) {
					groupInstance = (String) factoryConfig;
				}
			}
			if (StringUtils.hasText(KafkaMessageListenerContainer.this.clientIdSuffix)
					&& StringUtils.hasText(groupInstance)) {

				properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
						groupInstance + KafkaMessageListenerContainer.this.clientIdSuffix);
			}
		}

		private boolean setupSubBatchPerPartition() {
			Boolean subBatching = this.containerProperties.getSubBatchPerPartition();
			if (subBatching != null) {
				return subBatching;
			}
			if (this.transactionManager == null) {
				return false;
			}
			return this.eosMode.equals(EOSMode.ALPHA);
		}

		@Nullable
		private DeliveryAttemptAware setupDeliveryAttemptAware() {
			DeliveryAttemptAware aware = null;
			if (this.containerProperties.isDeliveryAttemptHeader()) {
				if (this.transactionManager != null) {
					if (getAfterRollbackProcessor() instanceof DeliveryAttemptAware) {
						aware = (DeliveryAttemptAware) getAfterRollbackProcessor();
					}
				}
				else {
					if (this.commonErrorHandler.deliveryAttemptHeader()) {
						aware = this.commonErrorHandler;
					}
				}
			}
			return aware;
		}

		private boolean determineCommitCurrent(Properties consumerProperties, Map<String, Object> factoryConfigs) {
			if (AssignmentCommitOption.NEVER.equals(this.autoCommitOption)) {
				return false;
			}
			if (!this.autoCommit && AssignmentCommitOption.ALWAYS.equals(this.autoCommitOption)) {
				return true;
			}
			String autoOffsetReset = consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
			if (autoOffsetReset == null) {
				Object config = factoryConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
				if (config instanceof String) {
					autoOffsetReset = (String) config;
				}
			}
			boolean resetLatest = autoOffsetReset == null || autoOffsetReset.equals("latest");
			boolean latestOnlyOption = AssignmentCommitOption.LATEST_ONLY.equals(this.autoCommitOption)
					|| AssignmentCommitOption.LATEST_ONLY_NO_TX.equals(this.autoCommitOption);
			return !this.autoCommit && resetLatest && latestOnlyOption;
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
							+ "'; using Kafka default.");
				}
				return (int) CONSUMER_CONFIG_DEFAULTS.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
			}
		}

		@Nullable
		private ConsumerSeekAware checkConsumerSeekAware(GenericMessageListener<?> candidate) {
			return candidate instanceof ConsumerSeekAware ? (ConsumerSeekAware) candidate : null;
		}

		boolean isConsumerPaused() {
			return this.consumerPaused;
		}

		boolean isPartitionPaused(TopicPartition topicPartition) {
			return this.pausedPartitions.contains(topicPartition);
		}

		@Nullable
		private TransactionTemplate determineTransactionTemplate() {
			if (this.kafkaTxManager != null) {
				this.producerPerConsumerPartition =
						this.kafkaTxManager.getProducerFactory().isProducerPerConsumerPartition();
			}
			if (this.transactionManager != null) {
				TransactionTemplate template = new TransactionTemplate(this.transactionManager);
				TransactionDefinition definition = this.containerProperties.getTransactionDefinition();
				Assert.state(definition == null
						|| definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRED
						|| definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRES_NEW,
						"Transaction propagation behavior must be REQUIRED or REQUIRES_NEW");
				if (definition != null) {
					BeanUtils.copyProperties(definition, template);
				}
				return template;
			}
			else {
				return null;
			}
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
			Duration syncTimeout = this.containerProperties.getSyncCommitTimeout();
			if (syncTimeout != null) {
				return syncTimeout;
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
							+ "'; defaulting to Kafka default for sync commit timeouts");
					}
					return Duration
							.ofMillis((int) CONSUMER_CONFIG_DEFAULTS.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
				}
			}
		}

		@Nullable
		private Object findDeserializerClass(Map<String, Object> props, Properties consumerOverrides, boolean isValue) {
			Object configuredDeserializer = isValue
					? KafkaMessageListenerContainer.this.consumerFactory.getValueDeserializer()
					: KafkaMessageListenerContainer.this.consumerFactory.getKeyDeserializer();
			if (configuredDeserializer == null) {
				Object deser = consumerOverrides.get(isValue
						? ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
						: ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
				if (deser == null) {
					deser = props.get(isValue
							? ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
							: ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
				}
				return deser;
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
				this.definedPartitions = new LinkedHashMap<>(topicPartitionsToAssign.size());
				for (TopicPartitionOffset topicPartition : topicPartitionsToAssign) {
					this.definedPartitions.put(topicPartition.getTopicPartition(),
							new OffsetMetadata(topicPartition.getOffset(), topicPartition.isRelativeToCurrent(),
									topicPartition.getPosition()));
				}
				subscribingConsumer.assign(new ArrayList<>(this.definedPartitions.keySet()));
			}
		}

		private boolean checkDeserializer(@Nullable Object deser) {
			Class<?> deserializer = null;
			if (deser instanceof Class) {
				deserializer = (Class<?>) deser;
			}
			else if (deser instanceof String) {
				try {
					ApplicationContext applicationContext = getApplicationContext();
					ClassLoader classLoader = applicationContext == null
							? getClass().getClassLoader()
							: applicationContext.getClassLoader();
					deserializer = ClassUtils.forName((String) deser, classLoader);
				}
				catch (ClassNotFoundException | LinkageError e) {
					throw new IllegalStateException(e);
				}
			}
			else if (deser != null) {
				throw new IllegalStateException("Deserializer must be a class or class name, not a " + deser.getClass());
			}
			return deserializer == null ? false : ErrorHandlingDeserializer.class.isAssignableFrom(deserializer);
		}

		protected void checkConsumer() {
			long timeSinceLastPoll = System.currentTimeMillis() - this.lastPoll;
			if (((float) timeSinceLastPoll) / (float) this.containerProperties.getPollTimeout()
					> this.containerProperties.getNoPollThreshold()) {
				publishNonResponsiveConsumerEvent(timeSinceLastPoll, this.consumer);
			}
		}

		@Nullable
		private MicrometerHolder obtainMicrometerHolder() {
			MicrometerHolder holder = null;
			try {
				if (KafkaUtils.MICROMETER_PRESENT && this.containerProperties.isMicrometerEnabled()) {
					holder = new MicrometerHolder(getApplicationContext(), getBeanName(),
							"spring.kafka.listener", "Kafka Listener Timer",
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

		@Override // NOSONAR complexity
		public void run() {
			ListenerUtils.setLogOnlyMetadata(this.containerProperties.isOnlyLogRecordMetadata());
			publishConsumerStartingEvent();
			this.consumerThread = Thread.currentThread();
			setupSeeks();
			KafkaUtils.setConsumerGroupId(this.consumerGroupId);
			this.count = 0;
			this.last = System.currentTimeMillis();
			initAssignedPartitions();
			publishConsumerStartedEvent();
			Throwable exitThrowable = null;
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
					exitThrowable = nofpe;
					break;
				}
				catch (AuthorizationException ae) {
					if (this.authorizationExceptionRetryInterval == null) {
						ListenerConsumer.this.logger.error(ae, "Authorization Exception and no authorizationExceptionRetryInterval set");
						this.fatalError = true;
						exitThrowable = ae;
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
				catch (FencedInstanceIdException fie) {
					this.fatalError = true;
					ListenerConsumer.this.logger.error(fie, "'" + ConsumerConfig.GROUP_INSTANCE_ID_CONFIG
							+ "' has been fenced");
					exitThrowable = fie;
					break;
				}
				catch (StopAfterFenceException e) {
					this.logger.error(e, "Stopping container due to fencing");
					stop(false);
					exitThrowable = e;
				}
				catch (Error e) { // NOSONAR - rethrown
					Runnable runnable = KafkaMessageListenerContainer.this.emergencyStop;
					if (runnable != null) {
						runnable.run();
					}
					this.logger.error(e, "Stopping container due to an Error");
					wrapUp(e);
					throw e;
				}
				catch (Exception e) {
					handleConsumerException(e);
				}
			}
			wrapUp(exitThrowable);
		}

		private void setupSeeks() {
			if (this.consumerSeekAwareListener != null) {
				this.consumerSeekAwareListener.registerSeekCallback(this);
			}
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
			fixTxOffsetsIfNeeded();
			idleBetweenPollIfNecessary();
			if (this.seeks.size() > 0) {
				processSeeks();
			}
			pauseConsumerIfNecessary();
			pausePartitionsIfNecessary();
			this.lastPoll = System.currentTimeMillis();
			if (!isRunning()) {
				return;
			}
			this.polling.set(true);
			ConsumerRecords<K, V> records = doPoll();
			if (!this.polling.compareAndSet(true, false) && records != null) {
				/*
				 * There is a small race condition where wakeIfNecessary was called between
				 * exiting the poll and before we reset the boolean.
				 */
				if (records.count() > 0) {
					this.logger.debug(() -> "Discarding polled records, container stopped: " + records.count());
				}
				return;
			}
			debugRecords(records);
			resumeConsumerIfNeccessary();
			if (!this.consumerPaused) {
				resumePartitionsIfNecessary();
			}

			invokeIfHaveRecords(records);
		}

		private void invokeIfHaveRecords(@Nullable ConsumerRecords<K, V> records) {
			if (records != null && records.count() > 0) {
				this.receivedSome = true;
				savePositionsIfNeeded(records);
				notIdle();
				notIdlePartitions(records.partitions());
				invokeListener(records);
			}
			else {
				checkIdle();
			}
			if (records == null || records.count() == 0
					|| records.partitions().size() < this.consumer.assignment().size()) {
				checkIdlePartitions();
			}
		}

		private void checkIdlePartitions() {
			Set<TopicPartition> partitions = this.consumer.assignment();
			partitions.forEach(this::checkIdlePartition);
		}

		private void checkIdlePartition(TopicPartition topicPartition) {
			Long idlePartitionEventInterval = this.containerProperties.getIdlePartitionEventInterval();
			if (idlePartitionEventInterval != null) {
				long now = System.currentTimeMillis();
				Long lstReceive = this.lastReceivePartition.computeIfAbsent(topicPartition, newTopicPartition -> now);
				Long lstAlertAt = this.lastAlertPartition.computeIfAbsent(topicPartition, newTopicPartition -> now);
				if (now > lstReceive + idlePartitionEventInterval
						&& now > lstAlertAt + idlePartitionEventInterval) {
					this.wasIdlePartition.put(topicPartition, true);
					publishIdlePartitionEvent(now - lstReceive, topicPartition, this.consumer,
							isPartitionPauseRequested(topicPartition));
					this.lastAlertPartition.put(topicPartition, now);
					if (this.consumerSeekAwareListener != null) {
						seekPartitions(Collections.singletonList(topicPartition), true);
					}
				}
			}
		}

		private void notIdlePartitions(Set<TopicPartition> partitions) {
			if (this.containerProperties.getIdlePartitionEventInterval() != null) {
				partitions.forEach(this::notIdlePartition);
			}
		}

		private void notIdlePartition(TopicPartition topicPartition) {
			long now = System.currentTimeMillis();
			Boolean partitionWasIdle = this.wasIdlePartition.get(topicPartition);
			if (partitionWasIdle != null && partitionWasIdle) {
				this.wasIdlePartition.put(topicPartition, false);
				Long lstReceive = this.lastReceivePartition.computeIfAbsent(topicPartition, newTopicPartition -> now);
				publishNoLongerIdlePartitionEvent(now - lstReceive, this.consumer, topicPartition);
			}
			this.lastReceivePartition.put(topicPartition, now);
		}

		private void notIdle() {
			if (this.containerProperties.getIdleEventInterval() != null) {
				long now = System.currentTimeMillis();
				if (this.wasIdle) {
					this.wasIdle = false;
					publishNoLongerIdleContainerEvent(now - this.lastReceive, this.consumer);
				}
				this.lastReceive = now;
			}
		}

		private void savePositionsIfNeeded(ConsumerRecords<K, V> records) {
			if (this.fixTxOffsets) {
				this.savedPositions.clear();
				records.partitions().forEach(tp -> this.savedPositions.put(tp, this.consumer.position(tp)));
			}
		}

		@SuppressWarnings("rawtypes")
		private void fixTxOffsetsIfNeeded() {
			if (this.fixTxOffsets) {
				try {
					Map<TopicPartition, OffsetAndMetadata> toFix = new HashMap<>();
					this.lastCommits.forEach((tp, oamd) -> {
						long position = this.consumer.position(tp);
						Long saved = this.savedPositions.get(tp);
						if (saved != null && saved.longValue() != position) {
							this.logger.debug(() -> "Skipping TX offset correction - seek(s) have been performed; "
									+ "saved: " + this.savedPositions + ", "
									+ "comitted: " + oamd + ", "
									+ "current: " + tp + "@" + position);
							return;
						}
						if (position > oamd.offset()) {
							toFix.put(tp, new OffsetAndMetadata(position));
						}
					});
					if (toFix.size() > 0) {
						this.logger.debug(() -> "Fixing TX offsets: " + toFix);
						if (this.kafkaTxManager == null) {
							if (this.syncCommits) {
								commitSync(toFix);
							}
							else {
								commitAsync(toFix, 0);
							}
						}
						else {
							this.transactionTemplate.executeWithoutResult(status -> {
								doSendOffsets(((KafkaResourceHolder) TransactionSynchronizationManager
										.getResource(this.kafkaTxManager.getProducerFactory()))
										.getProducer(), toFix);
							});
						}
					}
				}
				catch (Exception e) {
					this.logger.error(e, () -> "Failed to correct transactional offset(s): "
							+ ListenerConsumer.this.lastCommits);
				}
				finally {
					ListenerConsumer.this.lastCommits.clear();
				}
			}
		}

		@Nullable
		private ConsumerRecords<K, V> doPoll() {
			ConsumerRecords<K, V> records;
			if (this.isBatchListener && this.subBatchPerPartition) {
				if (this.batchIterator == null) {
					this.lastBatch = this.consumer.poll(this.pollTimeout);
					captureOffsets(this.lastBatch);
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
				captureOffsets(records);
				checkRebalanceCommits();
			}
			return records;
		}

		private synchronized void captureOffsets(ConsumerRecords<K, V> records) {
			if (this.offsetsInThisBatch != null && records.count() > 0) {
				this.offsetsInThisBatch.clear();
				this.deferredOffsets.clear();
				records.partitions().forEach(part -> {
					LinkedList<Long> offs = new LinkedList<>();
					this.offsetsInThisBatch.put(part, offs);
					this.deferredOffsets.put(part, new LinkedList<>());
					records.records(part).forEach(rec -> offs.add(rec.offset()));
				});
			}
		}

		private void checkRebalanceCommits() {
			if (this.commitsDuringRebalance.size() > 0) {
				// Attempt to recommit the offsets for partitions that we still own
				Map<TopicPartition, OffsetAndMetadata> commits = this.commitsDuringRebalance.entrySet()
						.stream()
						.filter(entry -> this.assignedPartitions.contains(entry.getKey()))
						.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
				this.commitsDuringRebalance.clear();
				this.logger.debug(() -> "Commit list: " + commits);
				commitSync(commits);
			}
		}

		void wakeIfNecessary() {
			if (this.polling.getAndSet(false)) {
				this.consumer.wakeup();
			}
		}

		private void debugRecords(@Nullable ConsumerRecords<K, V> records) {
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
				ListenerUtils.stoppableSleep(KafkaMessageListenerContainer.this, duration.toMillis());
			}
			catch (InterruptedException e) {
				this.logger.error(e, "Interrupted while sleeping");
			}
		}

		private void pauseConsumerIfNecessary() {
			if (this.offsetsInThisBatch != null) {
				synchronized (this) {
					doPauseConsumerIfNecessary();
				}
			}
			else {
				doPauseConsumerIfNecessary();
			}
		}

		private void doPauseConsumerIfNecessary() {
			if (this.offsetsInThisBatch != null && this.offsetsInThisBatch.size() > 0 && !this.pausedForAsyncAcks) {
				this.pausedForAsyncAcks = true;
				this.logger.debug(() -> "Pausing for incomplete async acks: " + this.offsetsInThisBatch);
			}
			if (!this.consumerPaused && (isPaused() || this.pausedForAsyncAcks)) {
				this.consumer.pause(this.consumer.assignment());
				this.consumerPaused = true;
				this.logger.debug(() -> "Paused consumption from: " + this.consumer.paused());
				publishConsumerPausedEvent(this.consumer.assignment());
			}
		}

		private void resumeConsumerIfNeccessary() {
			if (this.offsetsInThisBatch != null) {
				synchronized (this) {
					doResumeConsumerIfNeccessary();
				}
			}
			else {
				doResumeConsumerIfNeccessary();
			}
		}

		private void doResumeConsumerIfNeccessary() {
			if (this.pausedForAsyncAcks && this.offsetsInThisBatch.size() == 0) {
				this.pausedForAsyncAcks = false;
				this.logger.debug("Resuming after manual async acks cleared");
			}
			if (this.consumerPaused && !isPaused() && !this.pausedForAsyncAcks) {
				this.logger.debug(() -> "Resuming consumption from: " + this.consumer.paused());
				Collection<TopicPartition> paused = new LinkedList<>(this.consumer.paused());
				paused.removeAll(this.pausedPartitions);
				this.consumer.resume(paused);
				this.consumerPaused = false;
				publishConsumerResumedEvent(paused);
			}
		}

		private void pausePartitionsIfNecessary() {
			Set<TopicPartition> pausedConsumerPartitions = this.consumer.paused();
			Collection<TopicPartition> partitions = getAssignedPartitions();
			if (partitions != null) {
				List<TopicPartition> partitionsToPause = partitions
						.stream()
						.filter(tp -> isPartitionPauseRequested(tp)
								&& !pausedConsumerPartitions.contains(tp))
						.collect(Collectors.toList());
				if (partitionsToPause.size() > 0) {
					this.consumer.pause(partitionsToPause);
					this.pausedPartitions.addAll(partitionsToPause);
					this.logger.debug(() -> "Paused consumption from " + partitionsToPause);
					partitionsToPause.forEach(KafkaMessageListenerContainer.this::publishConsumerPartitionPausedEvent);
				}
			}
		}

		private void resumePartitionsIfNecessary() {
			Set<TopicPartition> pausedConsumerPartitions = this.consumer.paused();
			List<TopicPartition> partitionsToResume = this
					.assignedPartitions
					.stream()
					.filter(tp -> !isPartitionPauseRequested(tp)
							&& pausedConsumerPartitions.contains(tp))
					.collect(Collectors.toList());
			if (partitionsToResume.size() > 0) {
				this.consumer.resume(partitionsToResume);
				this.pausedPartitions.removeAll(partitionsToResume);
				this.logger.debug(() -> "Resumed consumption from " + partitionsToResume);
				partitionsToResume.forEach(KafkaMessageListenerContainer.this::publishConsumerPartitionResumedEvent);
			}
		}

		private void checkIdle() {
			Long idleEventInterval = this.containerProperties.getIdleEventInterval();
			if (idleEventInterval != null) {
				long idleEventInterval2 = idleEventInterval;
				long now = System.currentTimeMillis();
				if (!this.receivedSome) {
					idleEventInterval2 *= this.containerProperties.getIdleBeforeDataMultiplier();
				}
				if (now > this.lastReceive + idleEventInterval2
						&& now > this.lastAlertAt + idleEventInterval2) {
					this.wasIdle = true;
					publishIdleContainerEvent(now - this.lastReceive, this.consumer, this.consumerPaused);
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
			Collection<TopicPartition> assigned = getAssignedPartitions();
			if (idleBetweenPolls > 0 && assigned != null && assigned.size() > 0) {
				idleBetweenPolls = Math.min(idleBetweenPolls,
						this.maxPollInterval - (System.currentTimeMillis() - this.lastPoll)
								- 5000); // NOSONAR - less by five seconds to avoid race condition with rebalance
				if (idleBetweenPolls > 0) {
					try {
						ListenerUtils.stoppableSleep(KafkaMessageListenerContainer.this, idleBetweenPolls);
					}
					catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						throw new IllegalStateException("Consumer Thread [" + this + "] has been interrupted", ex);
					}
				}
			}
		}

		private void wrapUp(@Nullable Throwable throwable) {
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
				KafkaMessageListenerContainer.this.stop(false);
			}
			this.monitorTask.cancel(true);
			if (!this.taskSchedulerExplicitlySet) {
				((ThreadPoolTaskScheduler) this.taskScheduler).destroy();
			}
			this.consumer.close();
			getAfterRollbackProcessor().clearThreadState();
			if (this.commonErrorHandler != null) {
				this.commonErrorHandler.clearThreadState();
			}
			if (this.consumerSeekAwareListener != null) {
				this.consumerSeekAwareListener.onPartitionsRevoked(partitions);
				this.consumerSeekAwareListener.unregisterSeekCallback();
			}
			this.logger.info(() -> getGroupId() + ": Consumer stopped");
			publishConsumerStoppedEvent(throwable);
		}

		/**
		 * Handle exceptions thrown by the consumer outside of message listener
		 * invocation (e.g. commit exceptions).
		 * @param e the exception.
		 */
		protected void handleConsumerException(Exception e) {
			if (e instanceof RetriableCommitFailedException) {
				this.logger.error(e, "Commit retries exhausted");
				return;
			}
			try {
				if (this.commonErrorHandler != null) {
						this.commonErrorHandler.handleOtherException(e, this.consumer,
								KafkaMessageListenerContainer.this.thisOrParentContainer, this.isBatchListener);
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
			this.logger.trace(() -> "Ack: " + ListenerUtils.recordToString(record, true));
		}

		private void doAck(ConsumerRecord<K, V> record) {
			traceAck(record);
			if (this.offsetsInThisBatch != null) { // NOSONAR (sync)
				ackInOrder(record);
			}
			else {
				processAck(record);
			}
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

		private void processAcks(ConsumerRecords<K, V> records) {
			if (!Thread.currentThread().equals(this.consumerThread)) {
				try {
					for (ConsumerRecord<K, V> record : records) {
						this.acks.put(record);
					}
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
						ackImmediate(records);
					}
					catch (@SuppressWarnings(UNUSED) WakeupException e) {
						// ignore - not polling
					}
				}
				else {
					for (ConsumerRecord<K, V> record : records) {
						addOffset(record);
					}
				}
			}
		}

		private synchronized void ackInOrder(ConsumerRecord<K, V> record) {
			TopicPartition part = new TopicPartition(record.topic(), record.partition());
			List<Long> offs = this.offsetsInThisBatch.get(part);
			List<ConsumerRecord<K, V>> deferred = this.deferredOffsets.get(part);
			if (offs.size() > 0) {
				if (offs.get(0) == record.offset()) {
					offs.remove(0);
					ConsumerRecord<K, V> recordToAck = record;
					if (deferred.size() > 0) {
						Collections.sort(deferred, (a, b) -> Long.compare(a.offset(), b.offset()));
						while (deferred.size() > 0 && deferred.get(0).offset() == recordToAck.offset() + 1) {
							recordToAck = deferred.remove(0);
							offs.remove(0);
						}
					}
					processAck(recordToAck);
					if (offs.size() == 0) {
						this.deferredOffsets.remove(part);
						this.offsetsInThisBatch.remove(part);
					}
				}
				else if (record.offset() < offs.get(0)) {
					throw new IllegalStateException("First remaining offset for this batch is " + offs.get(0)
							+ "; you are acknowledging a stale record: " + ListenerUtils.recordToString(record));
				}
				else {
					deferred.add(record);
				}
			}
			else {
				throw new IllegalStateException("Unexpected ack for " + ListenerUtils.recordToString(record)
						+ "; offsets list is empty");
			}
		}

		private void ackImmediate(ConsumerRecord<K, V> record) {
			Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
					new TopicPartition(record.topic(), record.partition()),
					new OffsetAndMetadata(record.offset() + 1));
			this.commitLogger.log(() -> "Committing: " + commits);
			if (this.producer != null) {
				doSendOffsets(this.producer, commits);
			}
			else if (this.syncCommits) {
				commitSync(commits);
			}
			else {
				commitAsync(commits, 0);
			}
		}

		private void ackImmediate(ConsumerRecords<K, V> records) {
			Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
			for (TopicPartition part : records.partitions()) {
				commits.put(part,
						new OffsetAndMetadata(records.records(part)
								.get(records.records(part).size() - 1).offset() + 1));
			}
			this.commitLogger.log(() -> "Committing: " + commits);
			if (this.producer != null) {
				doSendOffsets(this.producer, commits);
			}
			else if (this.syncCommits) {
				commitSync(commits);
			}
			else {
				commitAsync(commits, 0);
			}
		}

		private void commitAsync(Map<TopicPartition, OffsetAndMetadata> commits, int retries) {
			this.consumer.commitAsync(commits, (offsetsAttempted, exception) -> {
				if (exception instanceof RetriableCommitFailedException
						&& retries < this.containerProperties.getCommitRetries()) {
					commitAsync(commits, retries + 1);
				}
				else {
					this.commitCallback.onComplete(offsetsAttempted, exception);
					if (this.fixTxOffsets) {
						this.lastCommits.putAll(commits);
					}
				}
			});
		}

		private void invokeListener(final ConsumerRecords<K, V> records) {
			if (this.isBatchListener) {
				invokeBatchListener(records);
			}
			else {
				invokeRecordListener(records);
			}
		}

		private void invokeBatchListener(final ConsumerRecords<K, V> recordsArg) {
			ConsumerRecords<K, V> records = checkEarlyIntercept(recordsArg);
			if (records == null || records.count() == 0) {
				return;
			}
			List<ConsumerRecord<K, V>> recordList = null;
			if (!this.wantsFullRecords) {
				recordList = createRecordList(records);
			}
			if (this.wantsFullRecords || recordList.size() > 0) {
				if (this.transactionTemplate != null) {
					invokeBatchListenerInTx(records, recordList); // NOSONAR
				}
				else {
					doInvokeBatchListener(records, recordList); // NOSONAR
				}
			}
		}

		@SuppressWarnings(RAW_TYPES)
		private void invokeBatchListenerInTx(final ConsumerRecords<K, V> records,
				@Nullable final List<ConsumerRecord<K, V>> recordList) {

			try {
				if (this.subBatchPerPartition && this.producerPerConsumerPartition) {
					ConsumerRecord<K, V> record = recordList == null ? records.iterator().next() : recordList.get(0);
					TransactionSupport
							.setTransactionIdSuffix(zombieFenceTxIdSuffix(record.topic(), record.partition())); // NOSONAR
				}
				this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {

					@Override
					public void doInTransactionWithoutResult(TransactionStatus s) {
						if (ListenerConsumer.this.kafkaTxManager != null) {
							ListenerConsumer.this.producer = ((KafkaResourceHolder) TransactionSynchronizationManager
									.getResource(ListenerConsumer.this.kafkaTxManager.getProducerFactory()))
										.getProducer(); // NOSONAR nullable
						}
						RuntimeException aborted = doInvokeBatchListener(records, recordList);
						if (aborted != null) {
							throw aborted;
						}
					}
				});
			}
			catch (ProducerFencedException | FencedInstanceIdException e) {
				this.logger.error(e, "Producer or '"
						+ ConsumerConfig.GROUP_INSTANCE_ID_CONFIG
						+ "' fenced during transaction");
				if (this.containerProperties.isStopContainerWhenFenced()) {
					throw new StopAfterFenceException("Container stopping due to fencing", e);
				}
			}
			catch (RuntimeException e) {
				this.logger.error(e, "Transaction rolled back");
				batchRollback(records, recordList, e);
			}
			finally {
				if (this.subBatchPerPartition && this.producerPerConsumerPartition) {
					TransactionSupport.clearTransactionIdSuffix();
				}
			}
		}

		private void batchRollback(final ConsumerRecords<K, V> records,
				@Nullable final List<ConsumerRecord<K, V>> recordList, RuntimeException e) {

			@SuppressWarnings(UNCHECKED)
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

		private void batchAfterRollback(final ConsumerRecords<K, V> records,
				@Nullable final List<ConsumerRecord<K, V>> recordList, RuntimeException rollbackException,
				AfterRollbackProcessor<K, V> afterRollbackProcessorToUse) {

			try {
				if (recordList == null) {
					afterRollbackProcessorToUse.process(createRecordList(records), this.consumer,
							KafkaMessageListenerContainer.this.thisOrParentContainer,  rollbackException, false,
							this.eosMode);
				}
				else {
					afterRollbackProcessorToUse.process(recordList, this.consumer,
							KafkaMessageListenerContainer.this.thisOrParentContainer,  rollbackException, false,
							this.eosMode);
				}
			}
			catch (KafkaException ke) {
				ke.selfLog("AfterRollbackProcessor threw an exception", this.logger);
			}
			catch (Exception ex) {
				this.logger.error(ex, "AfterRollbackProcessor threw an exception");
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
		 * @return an exception.
		 * @throws Error an error.
		 */
		@Nullable
		private RuntimeException doInvokeBatchListener(final ConsumerRecords<K, V> records, // NOSONAR
				List<ConsumerRecord<K, V>> recordList) {

			Object sample = startMicrometerSample();
			try {
				invokeBatchOnMessage(records, recordList);
				batchInterceptAfter(records, null);
				successTimer(sample);
				if (this.batchFailed) {
					this.batchFailed = false;
					if (this.commonErrorHandler != null) {
						this.commonErrorHandler.clearThreadState();
					}
					getAfterRollbackProcessor().clearThreadState();
				}
			}
			catch (RuntimeException e) {
				failureTimer(sample);
				batchInterceptAfter(records, e);
				if (this.commonErrorHandler == null) {
					throw e;
				}
				try {
					this.batchFailed = true;
					invokeBatchErrorHandler(records, recordList, e);
					commitOffsetsIfNeeded(records);
				}
				catch (KafkaException ke) {
					ke.selfLog(ERROR_HANDLER_THREW_AN_EXCEPTION, this.logger);
					return ke;
				}
				catch (RuntimeException ee) {
					this.logger.error(ee, ERROR_HANDLER_THREW_AN_EXCEPTION);
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

		private void commitOffsetsIfNeeded(final ConsumerRecords<K, V> records) {
			if ((!this.autoCommit && this.commonErrorHandler.isAckAfterHandle())
					|| this.producer != null) {
				this.acks.addAll(getHighestOffsetRecords(records));
				if (this.producer != null) {
					sendOffsetsToTransaction();
				}
			}
		}

		private void batchInterceptAfter(ConsumerRecords<K, V> records, @Nullable Exception exception) {
			if (this.commonBatchInterceptor != null) {
				try {
					if (exception == null) {
						this.commonBatchInterceptor.success(records, this.consumer);
					}
					else {
						this.commonBatchInterceptor.failure(records, exception, this.consumer);
					}
				}
				catch (Exception e) {
					this.logger.error(e, "BatchInterceptor threw an exception");
				}
			}
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
				this.micrometerHolder.failure(sample, "ListenerExecutionFailedException");
			}
		}

		private void invokeBatchOnMessage(final ConsumerRecords<K, V> records, // NOSONAR - Cyclomatic Complexity
				List<ConsumerRecord<K, V>> recordList) throws InterruptedException {

			invokeBatchOnMessageWithRecordsOrList(records, recordList);
			List<ConsumerRecord<?, ?>> toSeek = null;
			if (this.nackSleep >= 0) {
				int index = 0;
				toSeek = new ArrayList<>();
				for (ConsumerRecord<K, V> record : records) {
					if (index++ >= this.nackIndex) {
						toSeek.add(record);
					}
				}
			}
			if (this.producer != null || (!this.isAnyManualAck && !this.autoCommit)) {
				if (this.nackSleep < 0) {
					for (ConsumerRecord<K, V> record : getHighestOffsetRecords(records)) {
						this.acks.put(record);
					}
				}
				if (this.producer != null) {
					sendOffsetsToTransaction();
				}
			}
			if (toSeek != null) {
				if (!this.autoCommit) {
					processCommits();
				}
				SeekUtils.doSeeks(toSeek, this.consumer, null, true, (rec, ex) -> false, this.logger); // NOSONAR
				nackSleepAndReset();
			}
		}

		private void invokeBatchOnMessageWithRecordsOrList(final ConsumerRecords<K, V> recordsArg,
				@Nullable List<ConsumerRecord<K, V>> recordListArg) {

			ConsumerRecords<K, V> records = recordsArg;
			List<ConsumerRecord<K, V>> recordList = recordListArg;
			if (this.batchInterceptor != null) {
				records = this.batchInterceptor.intercept(recordsArg, this.consumer);
				if (records == null) {
					this.logger.debug(() -> "BatchInterceptor returned null, skipping: "
							+ recordsArg + " with " + recordsArg.count() + " records");
					return;
				}
				else {
					recordList = createRecordList(records);
				}
			}
			if (this.wantsFullRecords) {
				this.batchListener.onMessage(records, // NOSONAR
						this.isAnyManualAck
								? new ConsumerBatchAcknowledgment(records)
								: null,
						this.consumer);
			}
			else {
				doInvokeBatchOnMessage(records, recordList); // NOSONAR
			}
		}

		private void doInvokeBatchOnMessage(final ConsumerRecords<K, V> records,
				List<ConsumerRecord<K, V>> recordList) {

			try {
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
			catch (Exception ex) { //  NOSONAR
				throw decorateException(ex);
			}
		}

		private void invokeBatchErrorHandler(final ConsumerRecords<K, V> records,
				@Nullable List<ConsumerRecord<K, V>> list, RuntimeException rte) {

			this.commonErrorHandler.handleBatch(rte, records, this.consumer,
					KafkaMessageListenerContainer.this.thisOrParentContainer,
					() -> invokeBatchOnMessageWithRecordsOrList(records, list));
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
				if (this.stopImmediate && !isRunning()) {
					break;
				}
				final ConsumerRecord<K, V> record = checkEarlyIntercept(iterator.next());
				if (record == null) {
					continue;
				}
				this.logger.trace(() -> "Processing " + ListenerUtils.recordToString(record));
				try {
					invokeInTransaction(iterator, record);
				}
				catch (ProducerFencedException | FencedInstanceIdException e) {
					this.logger.error(e, "Producer or 'group.instance.id' fenced during transaction");
					if (this.containerProperties.isStopContainerWhenFenced()) {
						throw new StopAfterFenceException("Container stopping due to fencing", e);
					}
					break;
				}
				catch (RuntimeException ex) {
					this.logger.error(ex, "Transaction rolled back");
					recordAfterRollback(iterator, record, ex);
				}
				finally {
					if (this.producerPerConsumerPartition) {
						TransactionSupport.clearTransactionIdSuffix();
					}
				}
				if (this.nackSleep >= 0) {
					handleNack(records, record);
					break;
				}

			}
		}

		private void invokeInTransaction(Iterator<ConsumerRecord<K, V>> iterator, final ConsumerRecord<K, V> record) {
			if (this.producerPerConsumerPartition) {
				TransactionSupport
						.setTransactionIdSuffix(zombieFenceTxIdSuffix(record.topic(), record.partition()));
			}
			this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {

				@Override
				public void doInTransactionWithoutResult(TransactionStatus s) {
					if (ListenerConsumer.this.kafkaTxManager != null) {
						ListenerConsumer.this.producer = ((KafkaResourceHolder) TransactionSynchronizationManager
								.getResource(ListenerConsumer.this.kafkaTxManager.getProducerFactory()))
										.getProducer(); // NOSONAR
					}
					RuntimeException aborted = doInvokeRecordListener(record, iterator);
					if (aborted != null) {
						throw aborted;
					}
				}

			});
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
						afterRollbackProcessorToUse.process(unprocessed, ListenerConsumer.this.consumer,
								KafkaMessageListenerContainer.this.thisOrParentContainer, e, true,
								ListenerConsumer.this.eosMode);
					}

				});
			}
			else {
				try {
					afterRollbackProcessorToUse.process(unprocessed, this.consumer,
							KafkaMessageListenerContainer.this.thisOrParentContainer, e, true, this.eosMode);
				}
				catch (KafkaException ke) {
					ke.selfLog("AfterRollbackProcessor threw an exception", this.logger);
				}
				catch (Exception ex) {
					this.logger.error(ex, "AfterRollbackProcessor threw exception");
				}
			}
		}

		private void doInvokeWithRecords(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext()) {
				if (this.stopImmediate && !isRunning()) {
					break;
				}
				final ConsumerRecord<K, V> record = checkEarlyIntercept(iterator.next());
				if (record == null) {
					continue;
				}
				this.logger.trace(() -> "Processing " + ListenerUtils.recordToString(record));
				doInvokeRecordListener(record, iterator);
				if (this.nackSleep >= 0) {
					handleNack(records, record);
					break;
				}
			}
		}

		@Nullable
		private ConsumerRecords<K, V> checkEarlyIntercept(ConsumerRecords<K, V> nextArg) {
			ConsumerRecords<K, V> next = nextArg;
			if (this.earlyBatchInterceptor != null) {
				next = this.earlyBatchInterceptor.intercept(next, this.consumer);
				if (next == null) {
					this.logger.debug(() -> "BatchInterceptor returned null, skipping: "
						+ nextArg + " with " + nextArg.count() + " records");
				}
			}
			return next;
		}

		@Nullable
		private ConsumerRecord<K, V> checkEarlyIntercept(ConsumerRecord<K, V> nextArg) {
			ConsumerRecord<K, V> next = nextArg;
			if (this.earlyRecordInterceptor != null) {
				next = this.earlyRecordInterceptor.intercept(next, this.consumer);
				if (next == null) {
					this.logger.debug(() -> "RecordInterceptor returned null, skipping: "
						+ ListenerUtils.recordToString(nextArg));
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
			SeekUtils.doSeeks(list, this.consumer, null, true, (rec, ex) -> false, this.logger); // NOSONAR
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
		 * @param iterator the {@link ConsumerRecords} iterator - used only if a
		 * {@link RemainingRecordsErrorHandler} is being used.
		 * @return an exception.
		 * @throws Error an error.
		 */
		@Nullable
		private RuntimeException doInvokeRecordListener(final ConsumerRecord<K, V> record, // NOSONAR
				Iterator<ConsumerRecord<K, V>> iterator) {

			Object sample = startMicrometerSample();

			try {
				invokeOnMessage(record);
				successTimer(sample);
				recordInterceptAfter(record, null);
			}
			catch (RuntimeException e) {
				failureTimer(sample);
				recordInterceptAfter(record, e);
				if (this.commonErrorHandler == null) {
					throw e;
				}
				try {
					invokeErrorHandler(record, iterator, e);
					commitOffsetsIfNeeded(record);
				}
				catch (KafkaException ke) {
					if (ke.contains(KafkaBackoffException.class)) {
						this.logger.warn(ke.getMessage());
					}
					else {
						ke.selfLog(ERROR_HANDLER_THREW_AN_EXCEPTION, this.logger);
					}
					return ke;
				}
				catch (RuntimeException ee) {
					this.logger.error(ee, ERROR_HANDLER_THREW_AN_EXCEPTION);
					return ee;
				}
				catch (Error er) { // NOSONAR
					this.logger.error(er, "Error handler threw an error");
					throw er;
				}
			}
			return null;
		}

		private void commitOffsetsIfNeeded(final ConsumerRecord<K, V> record) {
			if ((!this.autoCommit && this.commonErrorHandler.isAckAfterHandle())
					|| this.producer != null) {
				if (this.isManualAck) {
					this.commitRecovered = true;
				}
				ackCurrent(record);
				if (this.isManualAck) {
					this.commitRecovered = false;
				}
			}
		}

		private void recordInterceptAfter(ConsumerRecord<K, V> records, @Nullable Exception exception) {
			if (this.commonRecordInterceptor != null) {
				try {
					if (exception == null) {
						this.commonRecordInterceptor.success(records, this.consumer);
					}
					else {
						this.commonRecordInterceptor.failure(records, exception, this.consumer);
					}
				}
				catch (Exception e) {
					this.logger.error(e, "RecordInterceptor threw an exception");
				}
			}
		}

		private void invokeOnMessage(final ConsumerRecord<K, V> record) {

			if (record.value() instanceof DeserializationException) {
				throw (DeserializationException) record.value();
			}
			if (record.key() instanceof DeserializationException) {
				throw (DeserializationException) record.key();
			}
			if (record.value() == null && this.checkNullValueForExceptions) {
				checkDeser(record, ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER);
			}
			if (record.key() == null && this.checkNullKeyForExceptions) {
				checkDeser(record, ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER);
			}
			if (this.deliveryAttemptAware != null) {
				byte[] buff = new byte[4]; // NOSONAR (magic #)
				ByteBuffer bb = ByteBuffer.wrap(buff);
				bb.putInt(this.deliveryAttemptAware
						.deliveryAttempt(
								new TopicPartitionOffset(record.topic(), record.partition(), record.offset())));
				record.headers().add(new RecordHeader(KafkaHeaders.DELIVERY_ATTEMPT, buff));
			}
			doInvokeOnMessage(record);
			if (this.nackSleep < 0 && !this.isManualImmediateAck) {
				ackCurrent(record);
			}
		}

		private void doInvokeOnMessage(final ConsumerRecord<K, V> recordArg) {
			ConsumerRecord<K, V> record = recordArg;
			if (this.recordInterceptor != null) {
				record = this.recordInterceptor.intercept(record, this.consumer);
			}
			if (record == null) {
				this.logger.debug(() -> "RecordInterceptor returned null, skipping: "
						+ ListenerUtils.recordToString(recordArg));
			}
			else {
				try {
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
				catch (Exception ex) { // NOSONAR
					throw decorateException(ex);
				}
			}
		}

		private void invokeErrorHandler(final ConsumerRecord<K, V> record,
				Iterator<ConsumerRecord<K, V>> iterator, RuntimeException rte) {

			if (this.commonErrorHandler.remainingRecords()) {
				if (this.producer == null) {
					processCommits();
				}
				List<ConsumerRecord<?, ?>> records = new ArrayList<>();
				records.add(record);
				while (iterator.hasNext()) {
					records.add(iterator.next());
				}
				this.commonErrorHandler.handleRemaining(rte, records, this.consumer,
						KafkaMessageListenerContainer.this.thisOrParentContainer);
			}
			else {
				this.commonErrorHandler.handleRecord(rte, record, this.consumer,
						KafkaMessageListenerContainer.this.thisOrParentContainer);
			}
		}

		private RuntimeException decorateException(Exception ex) {
			Exception toHandle = ex;
			if (toHandle instanceof ListenerExecutionFailedException) {
				toHandle = new ListenerExecutionFailedException(toHandle.getMessage(), this.consumerGroupId,
						toHandle.getCause()); // NOSONAR restored below
				fixStackTrace(ex, toHandle);
			}
			else {
				toHandle = new ListenerExecutionFailedException("Listener failed", this.consumerGroupId, toHandle);
			}
			return (RuntimeException) toHandle;
		}

		private void fixStackTrace(Exception ex, Exception toHandle) {
			try {
				StackTraceElement[] stackTrace = ex.getStackTrace();
				if (stackTrace != null && stackTrace.length > 0) {
					StackTraceElement[] stackTrace2 = toHandle.getStackTrace();
					if (stackTrace2 != null) {
						int matching = -1;
						for (int i = 0; i < stackTrace2.length; i++) {
							StackTraceElement se2 = stackTrace[i];
							for (StackTraceElement se : stackTrace2) {
								if (se2.equals(se)) {
									matching = i;
									break;
								}
							}
							if (matching >= 0) {
								break;
							}
						}
						if (matching >= 0) {
							StackTraceElement[] merged = new StackTraceElement[matching];
							System.arraycopy(stackTrace, 0, merged, 0, matching);
							ListenerExecutionFailedException suppressed =
									new ListenerExecutionFailedException("Restored Stack Trace");
							suppressed.setStackTrace(merged);
							toHandle.addSuppressed(suppressed);
						}
					}
				}
			}
			catch (Exception ex2) {
				this.logger.debug(ex2,
						"Could not restore the stack trace when decorating the LEFE with the group id");
			}
		}

		public void checkDeser(final ConsumerRecord<K, V> record, String headerName) {
			DeserializationException exception = ListenerUtils.getExceptionFromHeader(record, headerName, this.logger);
			if (exception != null) {
				/*
				 * Wrapping in a LEFE is not strictly correct, but required for backwards compatibility.
				 */
				throw decorateException(exception);
			}
		}

		public void ackCurrent(final ConsumerRecord<K, V> record) {

			if (this.isRecordAck) {
				Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
						Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
								new OffsetAndMetadata(record.offset() + 1));
				if (this.producer == null) {
					this.commitLogger.log(() -> "Committing: " + offsetsToCommit);
					if (this.syncCommits) {
						commitSync(offsetsToCommit);
					}
					else {
						commitAsync(offsetsToCommit, 0);
					}
				}
				else {
					this.acks.add(record);
				}
			}
			else if (this.producer != null
					|| ((!this.isAnyManualAck || this.commitRecovered) && !this.autoCommit)) {
				this.acks.add(record);
			}
			if (this.producer != null) {
				sendOffsetsToTransaction();
			}
		}

		private void sendOffsetsToTransaction() {
			handleAcks();
			Map<TopicPartition, OffsetAndMetadata> commits = buildCommits();
			this.commitLogger.log(() -> "Sending offsets to transaction: " + commits);
			doSendOffsets(this.producer, commits);
		}

		private void doSendOffsets(Producer<?, ?> prod, Map<TopicPartition, OffsetAndMetadata> commits) {
			if (this.eosMode.equals(EOSMode.ALPHA)) {
				prod.sendOffsetsToTransaction(commits, this.consumerGroupId);
			}
			else {
				prod.sendOffsetsToTransaction(commits, this.consumer.groupMetadata());
			}
			if (this.fixTxOffsets) {
				this.lastCommits.putAll(commits);
			}
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
						offsetsForTimes.forEach((tp, ot) -> {
							if (ot != null) {
								this.consumer.seek(tp, ot.offset());
							}
						});
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
				Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer.offsetsForTimes(timestampSeeks);
				offsetsForTimes.forEach((tp, ot) -> {
					if (ot != null) {
						this.consumer.seek(tp, ot.offset());
					}
				});
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
			Map<TopicPartition, OffsetMetadata> partitions = new LinkedHashMap<>(this.definedPartitions);
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
			Map<TopicPartition, Long> times = partitions.entrySet().stream()
					.filter(e -> SeekPosition.TIMESTAMP.equals(e.getValue().seekPosition))
					.collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().offset));
			Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer.offsetsForTimes(times);
			offsetsForTimes.forEach((tp, off) -> {
				if (off == null) {
					ends.add(tp);
				}
				else {
					partitions.put(tp, new OffsetMetadata(off.offset(), false, SeekPosition.TIMESTAMP));
				}
			});
			doInitialSeeks(partitions, beginnings, ends);
			if (this.consumerSeekAwareListener != null) {
				this.consumerSeekAwareListener.onPartitionsAssigned(partitions.keySet().stream()
							.map(tp -> new SimpleEntry<>(tp, this.consumer.position(tp)))
							.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())),
						this.seekCallback);
			}
		}

		private void doInitialSeeks(Map<TopicPartition, OffsetMetadata> partitions, Set<TopicPartition> beginnings,
				Set<TopicPartition> ends) {

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
						commitSync(commits);
					}
					else {
						commitAsync(commits, 0);
					}
				}
				catch (@SuppressWarnings(UNUSED) WakeupException e) {
					// ignore - not polling
					this.logger.debug("Woken up during commit");
				}
			}
		}

		private void commitSync(Map<TopicPartition, OffsetAndMetadata> commits) {
			doCommitSync(commits, 0);
		}

		private void doCommitSync(Map<TopicPartition, OffsetAndMetadata> commits, int retries) {
			try {
				this.consumer.commitSync(commits, this.syncCommitTimeout);
				if (this.fixTxOffsets) {
					this.lastCommits.putAll(commits);
				}
			}
			catch (RetriableCommitFailedException e) {
				if (retries >= this.containerProperties.getCommitRetries()) {
					throw e;
				}
				doCommitSync(commits, retries + 1);
			}
			catch (RebalanceInProgressException e) {
				this.logger.debug(e, "Non-fatal commit failure");
				this.commitsDuringRebalance.putAll(commits);
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
					+ "\ncontainerProperties=" + this.containerProperties
					+ "\nother properties ["
					+ "\n listenerType=" + this.listenerType
					+ "\n isConsumerAwareListener=" + this.isConsumerAwareListener
					+ "\n isBatchListener=" + this.isBatchListener
					+ "\n autoCommit=" + this.autoCommit
					+ "\n consumerGroupId=" + this.consumerGroupId
					+ "\n clientIdSuffix=" + KafkaMessageListenerContainer.this.clientIdSuffix
					+ "\n]";
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

			private volatile boolean acked;

			ConsumerAcknowledgment(ConsumerRecord<K, V> record) {
				this.record = record;
			}

			@Override
			public void acknowledge() {
				if (!this.acked) {
					doAck(this.record);
					this.acked = true;
				}
			}

			@Override
			public void nack(long sleep) {
				Assert.state(Thread.currentThread().equals(ListenerConsumer.this.consumerThread),
						"nack() can only be called on the consumer thread");
				Assert.isTrue(sleep >= 0, "sleep cannot be negative");
				ListenerConsumer.this.nackSleep = sleep;
				synchronized (ListenerConsumer.this) {
					if (ListenerConsumer.this.offsetsInThisBatch != null) {
						ListenerConsumer.this.offsetsInThisBatch.forEach((part, recs) -> recs.clear());
						ListenerConsumer.this.deferredOffsets.forEach((part, recs) -> recs.clear());
					}
				}
			}

			@Override
			public String toString() {
				return "Acknowledgment for " + ListenerUtils.recordToString(this.record, true);
			}

		}

		private final class ConsumerBatchAcknowledgment implements Acknowledgment {

			private final ConsumerRecords<K, V> records;

			private volatile boolean acked;

			ConsumerBatchAcknowledgment(ConsumerRecords<K, V> records) {
				this.records = records;
			}

			@Override
			public void acknowledge() {
				Map<TopicPartition, List<Long>> offs = ListenerConsumer.this.offsetsInThisBatch;
				Map<TopicPartition, List<ConsumerRecord<K, V>>> deferred = ListenerConsumer.this.deferredOffsets;
				if (!this.acked) {
					for (ConsumerRecord<K, V> record : getHighestOffsetRecords(this.records)) {
						if (offs != null) {
							offs.remove(new TopicPartition(record.topic(), record.partition()));
							deferred.remove(new TopicPartition(record.topic(), record.partition()));
						}
					}
					processAcks(this.records);
					this.acked = true;
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
				synchronized (ListenerConsumer.this) {
					if (ListenerConsumer.this.offsetsInThisBatch != null) {
						ListenerConsumer.this.offsetsInThisBatch.forEach((part, recs) -> recs.clear());
						ListenerConsumer.this.deferredOffsets.forEach((part, recs) -> recs.clear());
					}
				}
				int i = 0;
				List<ConsumerRecord<K, V>> toAck = new LinkedList<>();
				for (ConsumerRecord<K, V> record : this.records) {
					if (i++ < index) {
						toAck.add(record);
					}
					else {
						break;
					}
				}
				Map<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();
				for (ConsumerRecord<K, V> record : toAck) {
					newRecords.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
							tp -> new LinkedList<>()).add(record);
				}
				processAcks(new ConsumerRecords<K, V>(newRecords));
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
					try {
						// Wait until now to commit, in case the user listener added acks
						commitPendingAcks();
						fixTxOffsetsIfNeeded();
					}
					catch (Exception e) {
						ListenerConsumer.this.logger.error(e, () -> "Fatal commit error after revocation "
								+ partitions);
					}
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
					partitions.forEach(tp -> ListenerConsumer.this.lastCommits.remove(tp));
					synchronized (ListenerConsumer.this) {
						if (ListenerConsumer.this.offsetsInThisBatch != null) {
							partitions.forEach(tp -> {
								ListenerConsumer.this.offsetsInThisBatch.remove(tp);
								ListenerConsumer.this.deferredOffsets.remove(tp);
							});
						}
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
				ListenerConsumer.this.assignedPartitions.addAll(partitions);
				if (ListenerConsumer.this.commitCurrentOnAssignment
						&& !collectAndCommitIfNecessary(partitions)) {
					return;
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

			private boolean collectAndCommitIfNecessary(Collection<TopicPartition> partitions) {
				// Commit initial positions - this is generally redundant but
				// it protects us from the case when another consumer starts
				// and rebalance would cause it to reset at the end
				// see https://github.com/spring-projects/spring-kafka/issues/110
				Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
				Map<TopicPartition, OffsetAndMetadata> committed =
						ListenerConsumer.this.consumer.committed(new HashSet<>(partitions));
				for (TopicPartition partition : partitions) {
					try {
						if (committed.get(partition) == null) { // no existing commit for this group
							offsetsToCommit.put(partition,
									new OffsetAndMetadata(ListenerConsumer.this.consumer.position(partition)));
						}
					}
					catch (NoOffsetForPartitionException e) {
						ListenerConsumer.this.fatalError = true;
						ListenerConsumer.this.logger.error(e, "No offset and no reset policy");
						return false;
					}
				}
				if (offsetsToCommit.size() > 0) {
					commitCurrentOffsets(offsetsToCommit);
				}
				return true;
			}

			private void commitCurrentOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
				ListenerConsumer.this.commitLogger.log(() -> "Committing on assignment: " + offsetsToCommit);
				if (ListenerConsumer.this.transactionTemplate != null
						&& ListenerConsumer.this.kafkaTxManager != null
						&& !AssignmentCommitOption.LATEST_ONLY_NO_TX.equals(ListenerConsumer.this.autoCommitOption)) {
					try {
						offsetsToCommit.forEach((partition, offsetAndMetadata) -> {
							if (ListenerConsumer.this.producerPerConsumerPartition) {
								TransactionSupport.setTransactionIdSuffix(
									zombieFenceTxIdSuffix(partition.topic(), partition.partition()));
							}
							ListenerConsumer.this.transactionTemplate
									.execute(new TransactionCallbackWithoutResult() {

										@Override
										protected void doInTransactionWithoutResult(TransactionStatus status) {
											KafkaResourceHolder<?, ?> holder =
													(KafkaResourceHolder<?, ?>) TransactionSynchronizationManager
															.getResource(ListenerConsumer.this.kafkaTxManager
																	.getProducerFactory());
											if (holder != null) {
												doSendOffsets(holder.getProducer(),
															Collections.singletonMap(partition, offsetAndMetadata));
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
						try {
							ListenerConsumer.this.consumer.commitSync(offsetsToCommit,
									containerProps.getSyncCommitTimeout());
						}
						catch (RetriableCommitFailedException | RebalanceInProgressException e) {
							// ignore since this is on assignment anyway
						}
					}
					else {
						commitAsync(offsetsToCommit, 0);
					}
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

		final Long offset; // NOSONAR

		final boolean relativeToCurrent; // NOSONAR

		final SeekPosition seekPosition; // NOSONAR

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

	@SuppressWarnings("serial")
	private static class StopAfterFenceException extends KafkaException {

		StopAfterFenceException(String message, Throwable t) {
			super(message, t);
		}

	}

}
