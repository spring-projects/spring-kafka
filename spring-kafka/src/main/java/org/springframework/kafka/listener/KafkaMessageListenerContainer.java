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

package org.springframework.kafka.listener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Type;
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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ProducerFactoryUtils;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppingEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.support.TopicPartitionInitialOffset.SeekPosition;
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
 */
public class KafkaMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> { // NOSONAR comment density

	private static final int DEFAULT_ACK_TIME = 5000;

	private final AbstractMessageListenerContainer<K, V> container;

	private final TopicPartitionInitialOffset[] topicPartitions;

	private volatile ListenerConsumer listenerConsumer;

	private volatile ListenableFuture<?> listenerConsumerFuture;

	private String clientIdSuffix;

	private Runnable emergencyStop = () -> stop(() -> {
		// NOSONAR
	});

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties) {
		this(null, consumerFactory, containerProperties, (TopicPartitionInitialOffset[]) null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions/initialOffsets.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties, TopicPartitionInitialOffset... topicPartitions) {
		this(null, consumerFactory, containerProperties, topicPartitions);
	}

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param container a delegating container (if this is a sub-container).
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	KafkaMessageListenerContainer(AbstractMessageListenerContainer<K, V> container,
			ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties) {
		this(container, consumerFactory, containerProperties, (TopicPartitionInitialOffset[]) null);
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
			ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties, TopicPartitionInitialOffset... topicPartitions) {
		super(consumerFactory, containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.container = container == null ? this : container;
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
	public boolean isContainerPaused() {
		return isPaused() && this.listenerConsumer.consumerPaused;
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
		Assert.state(messageListener != null, "A MessageListener is required");
		if (containerProperties.getConsumerTaskExecutor() == null) {
			SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}
		Assert.state(messageListener instanceof GenericMessageListener, "Listener must be a GenericListener");
		GenericMessageListener<?> listener = (GenericMessageListener<?>) messageListener;
		ListenerType listenerType = deteremineListenerType(listener);
		this.listenerConsumer = new ListenerConsumer(listener, listenerType);
		setRunning(true);
		this.listenerConsumerFuture = containerProperties
				.getConsumerTaskExecutor()
				.submitListenable(this.listenerConsumer);
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

	private ListenerType deteremineListenerType(GenericMessageListener<?> listener) {
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
			this.listenerConsumer.consumer.wakeup();
		}
	}

	private void publishIdleContainerEvent(long idleTime, Consumer<?, ?> consumer, boolean paused) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ListenerContainerIdleEvent(this,
					this.container, idleTime, getBeanName(), getAssignedPartitions(), consumer, paused));
		}
	}

	private void publishNonResponsiveConsumerEvent(long timeSinceLastPoll, Consumer<?, ?> consumer) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(
					new NonResponsiveConsumerEvent(this, this.container, timeSinceLastPoll,
							getBeanName(), getAssignedPartitions(), consumer));
		}
	}

	private void publishConsumerPausedEvent(Collection<TopicPartition> partitions) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerPausedEvent(this, this.container,
					Collections.unmodifiableCollection(partitions)));
		}
	}

	private void publishConsumerResumedEvent(Collection<TopicPartition> partitions) {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerResumedEvent(this, this.container,
					Collections.unmodifiableCollection(partitions)));
		}
	}

	private void publishConsumerStoppingEvent(Consumer<?, ?> consumer) {
		try {
			if (getApplicationEventPublisher() != null) {
				getApplicationEventPublisher().publishEvent(
						new ConsumerStoppingEvent(this, this.container, consumer, getAssignedPartitions()));
			}
		}
		catch (Exception e) {
			this.logger.error("Failed to publish consumer stopping event", e);
		}
	}
	private void publishConsumerStoppedEvent() {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ConsumerStoppedEvent(this, this.container));
		}
	}

	@Override
	protected AbstractMessageListenerContainer<?, ?> parentOrThis() {
		return this.container;
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

		private static final String RAWTYPES = "rawtypes";

		private static final String RAW_TYPES = RAWTYPES;

		private final Log logger = LogFactory.getLog(ListenerConsumer.class);

		private final ContainerProperties containerProperties = getContainerProperties();

		private final OffsetCommitCallback commitCallback = this.containerProperties.getCommitCallback() != null
				? this.containerProperties.getCommitCallback()
				: new LoggingCommitCallback();

		private final Consumer<K, V> consumer;

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final GenericMessageListener<?> genericListener;

		private final MessageListener<K, V> listener;

		private final BatchMessageListener<K, V> batchListener;

		private final ListenerType listenerType;

		private final boolean isConsumerAwareListener;

		private final boolean isBatchListener;

		private final boolean wantsFullRecords;

		private final boolean autoCommit = KafkaMessageListenerContainer.this.consumerFactory.isAutoCommit();

		private final boolean isManualAck = this.containerProperties.getAckMode().equals(AckMode.MANUAL);

		private final boolean isCountAck = this.containerProperties.getAckMode().equals(AckMode.COUNT)
				|| this.containerProperties.getAckMode().equals(AckMode.COUNT_TIME);

		private final boolean isTimeOnlyAck = this.containerProperties.getAckMode().equals(AckMode.TIME);

		private final boolean isManualImmediateAck =
				this.containerProperties.getAckMode().equals(AckMode.MANUAL_IMMEDIATE);

		private final boolean isAnyManualAck = this.isManualAck || this.isManualImmediateAck;

		private final boolean isRecordAck = this.containerProperties.getAckMode().equals(AckMode.RECORD);

		private final BlockingQueue<ConsumerRecord<K, V>> acks = new LinkedBlockingQueue<>();

		private final BlockingQueue<TopicPartitionInitialOffset> seeks = new LinkedBlockingQueue<>();

		private final ErrorHandler errorHandler;

		private final BatchErrorHandler batchErrorHandler;

		private final PlatformTransactionManager transactionManager = this.containerProperties.getTransactionManager();

		@SuppressWarnings(RAW_TYPES)
		private final KafkaAwareTransactionManager kafkaTxManager =
				this.transactionManager instanceof KafkaAwareTransactionManager
						? ((KafkaAwareTransactionManager) this.transactionManager) : null;

		private final TransactionTemplate transactionTemplate;

		private final String consumerGroupId = this.containerProperties.getGroupId() == null
				? (String) KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties()
				.get(ConsumerConfig.GROUP_ID_CONFIG)
				: this.containerProperties.getGroupId();

		private final TaskScheduler taskScheduler;

		private final ScheduledFuture<?> monitorTask;

		private final LogIfLevelEnabled commitLogger = new LogIfLevelEnabled(this.logger,
				this.containerProperties.getCommitLogLevel());

		private final Duration pollTimeout = Duration.ofMillis(this.containerProperties.getPollTimeout());

		private final boolean checkNullKeyForExceptions;

		private final boolean checkNullValueForExceptions;

		private Map<TopicPartition, OffsetMetadata> definedPartitions;

		private volatile Collection<TopicPartition> assignedPartitions;

		private volatile Thread consumerThread;

		private int count;

		private long last = System.currentTimeMillis();

		private boolean fatalError;

		private boolean taskSchedulerExplicitlySet;

		private boolean consumerPaused;

		private long lastReceive = System.currentTimeMillis();

		private long lastAlertAt = this.lastReceive;

		private volatile long lastPoll = System.currentTimeMillis();

		@SuppressWarnings("unchecked")
		ListenerConsumer(GenericMessageListener<?> listener, ListenerType listenerType) {
			Assert.state(!this.isAnyManualAck || !this.autoCommit,
					"Consumer cannot be configured for auto commit for ackMode " + this.containerProperties.getAckMode());
			this.consumer =
					KafkaMessageListenerContainer.this.consumerFactory.createConsumer(
							this.consumerGroupId,
							this.containerProperties.getClientId(),
							KafkaMessageListenerContainer.this.clientIdSuffix);

			if (this.transactionManager != null) {
				this.transactionTemplate = new TransactionTemplate(this.transactionManager);
			}
			else {
				this.transactionTemplate = null;
			}
			subscribeOrAssignTopics(this.consumer);
			GenericErrorHandler<?> errHandler = KafkaMessageListenerContainer.this.getGenericErrorHandler();
			this.genericListener = listener;
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
			Assert.state(!this.isBatchListener || !this.isRecordAck, "Cannot use AckMode.RECORD with a batch listener");
			if (this.containerProperties.getScheduler() != null) {
				this.taskScheduler = this.containerProperties.getScheduler();
				this.taskSchedulerExplicitlySet = true;
			}
			else {
				ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
				threadPoolTaskScheduler.initialize();
				this.taskScheduler = threadPoolTaskScheduler;
			}
			this.monitorTask = this.taskScheduler.scheduleAtFixedRate(() -> checkConsumer(),
					this.containerProperties.getMonitorInterval() * 1000); // NOSONAR magic #
			if (this.containerProperties.isLogContainerConfig()) {
				this.logger.info(this);
			}
			Map<String, Object> props = KafkaMessageListenerContainer.this.consumerFactory.getConfigurationProperties();
			this.checkNullKeyForExceptions = checkDeserializer(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
			this.checkNullValueForExceptions = checkDeserializer(
					props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
		}

		private void subscribeOrAssignTopics(final Consumer<K, V> consumer) {
			if (KafkaMessageListenerContainer.this.topicPartitions == null) {
				ConsumerRebalanceListener rebalanceListener = new ListenerConsumerRebalanceListener();
				if (this.containerProperties.getTopicPattern() != null) {
					consumer.subscribe(this.containerProperties.getTopicPattern(), rebalanceListener);
				}
				else {
					consumer.subscribe(Arrays.asList(this.containerProperties.getTopics()), rebalanceListener);
				}
			}
			else {
				List<TopicPartitionInitialOffset> topicPartitionsToAssign =
						Arrays.asList(KafkaMessageListenerContainer.this.topicPartitions);
				this.definedPartitions = new HashMap<>(topicPartitionsToAssign.size());
				for (TopicPartitionInitialOffset topicPartition : topicPartitionsToAssign) {
					this.definedPartitions.put(topicPartition.topicPartition(),
							new OffsetMetadata(topicPartition.initialOffset(), topicPartition.isRelativeToCurrent(),
									topicPartition.getPosition()));
				}
				consumer.assign(new ArrayList<>(this.definedPartitions.keySet()));
			}
		}

		private boolean checkDeserializer(Object deser) {
			return deser instanceof Class
					? ((Class<?>) deser).equals(ErrorHandlingDeserializer2.class)
					: deser instanceof String
						? ((String) deser).equals(ErrorHandlingDeserializer2.class.getName())
						: false;
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

		private void seekPartitions(Collection<TopicPartition> partitions, boolean idle) {
			Map<TopicPartition, Long> current = new HashMap<>();
			for (TopicPartition topicPartition : partitions) {
				current.put(topicPartition, ListenerConsumer.this.consumer.position(topicPartition));
			}
			ConsumerSeekCallback callback = new ConsumerSeekCallback() {

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
				public void seekToEnd(String topic, int partition) {
					ListenerConsumer.this.consumer.seekToEnd(
							Collections.singletonList(new TopicPartition(topic, partition)));
				}

			};
			if (idle) {
				((ConsumerSeekAware) ListenerConsumer.this.genericListener).onIdleContainer(current, callback);
			}
			else {
				((ConsumerSeekAware) ListenerConsumer.this.genericListener).onPartitionsAssigned(current, callback);
			}
		}

		private void validateErrorHandler(boolean batch) {
			GenericErrorHandler<?> errHandler = KafkaMessageListenerContainer.this.getGenericErrorHandler();
			if (this.errorHandler == null) {
				return;
			}
			Type[] genericInterfaces = errHandler.getClass().getGenericInterfaces();
			boolean ok = false;
			for (Type t : genericInterfaces) {
				if (t.equals(ErrorHandler.class)) {
					ok = !batch;
					break;
				}
				else if (t.equals(BatchErrorHandler.class)) {
					ok = batch;
					break;
				}
			}
			Assert.state(ok, "Error handler is not compatible with the message listener, expecting an instance of "
					+ (batch ? "BatchErrorHandler" : "ErrorHandler") + " not " + errHandler.getClass().getName());
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			this.consumerThread = Thread.currentThread();
			if (this.genericListener instanceof ConsumerSeekAware) {
				((ConsumerSeekAware) this.genericListener).registerSeekCallback(this);
			}
			if (this.transactionManager != null) {
				ProducerFactoryUtils.setConsumerGroupId(this.consumerGroupId);
			}
			this.count = 0;
			this.last = System.currentTimeMillis();
			initAsignedPartitions();
			while (isRunning()) {
				try {
					pollAndInvoke();
				}
				catch (WakeupException e) {
					// Ignore, we're stopping
				}
				catch (NoOffsetForPartitionException nofpe) {
					this.fatalError = true;
					ListenerConsumer.this.logger.error("No offset and no reset policy", nofpe);
					break;
				}
				catch (Exception e) {
					handleConsumerException(e);
				}
				catch (Error e) { // NOSONAR - rethrown
					Runnable runnable = KafkaMessageListenerContainer.this.emergencyStop;
					if (runnable != null) {
						runnable.run();
					}
					this.logger.error("Stopping container due to an Error", e);
					wrapUp();
					throw e;
				}
			}
			wrapUp();
		}

		private void initAsignedPartitions() {
			if (isRunning() && this.definedPartitions != null) {
				try {
					initPartitionsIfNeeded();
				}
				catch (Exception e) {
					this.logger.error("Failed to set initial offsets", e);
				}
			}
		}

		protected void pollAndInvoke() {
			if (!this.autoCommit && !this.isRecordAck) {
				processCommits();
			}
			processSeeks();
			checkPaused();
			ConsumerRecords<K, V> records = this.consumer.poll(this.pollTimeout);
			this.lastPoll = System.currentTimeMillis();
			checkResumed();
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

		private void debugRecords(ConsumerRecords<K, V> records) {
			if (records != null && this.logger.isDebugEnabled()) {
				this.logger.debug("Received: " + records.count() + " records");
				if (records.count() > 0 && this.logger.isTraceEnabled()) {
					this.logger.trace(records.partitions().stream()
							.flatMap(p -> records.records(p).stream())
							// map to same format as send metadata toString()
							.map(r -> r.topic() + "-" + r.partition() + "@" + r.offset())
							.collect(Collectors.toList()));
				}
			}
		}

		private void checkPaused() {
			if (!this.consumerPaused && isPaused()) {
				this.consumer.pause(this.consumer.assignment());
				this.consumerPaused = true;
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Paused consumption from: " + this.consumer.paused());
				}
				publishConsumerPausedEvent(this.consumer.assignment());
			}
		}

		private void checkResumed() {
			if (this.consumerPaused && !isPaused()) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Resuming consumption from: " + this.consumer.paused());
				}
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
					if (this.genericListener instanceof ConsumerSeekAware) {
						Collection<TopicPartition> partitions = getAssignedPartitions();
						if (partitions != null) {
							seekPartitions(partitions, true);
						}
					}
				}
			}
		}

		public void wrapUp() {
			ProducerFactoryUtils.clearConsumerGroupId();
			publishConsumerStoppingEvent(this.consumer);
			if (!this.fatalError) {
				if (this.kafkaTxManager == null) {
					commitPendingAcks();
					try {
						this.consumer.unsubscribe();
					}
					catch (WakeupException e) {
						// No-op. Continue process
					}
				}
				else {
					closeProducers(getAssignedPartitions());
				}
			}
			else {
				this.logger.error("No offset and no reset policy; stopping container");
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
			this.logger.info("Consumer stopped");
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
							KafkaMessageListenerContainer.this);
				}
				else if (this.isBatchListener && this.batchErrorHandler != null) {
					this.batchErrorHandler.handle(e, new ConsumerRecords<K, V>(Collections.emptyMap()), this.consumer,
							KafkaMessageListenerContainer.this);
				}
				else {
					this.logger.error("Consumer exception", e);
				}
			}
			catch (Exception ex) {
				this.logger.error("Consumer exception", ex);
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
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Ack: " + record);
				}
				processAck(record);
				record = this.acks.poll();
			}
		}

		private void processAck(ConsumerRecord<K, V> record) {
			if (!Thread.currentThread().equals(this.consumerThread)) {
				try {
					this.acks.put(record);
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
					catch (WakeupException e) {
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
			if (this.containerProperties.isSyncCommits()) {
				this.consumer.commitSync(commits);
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

		@SuppressWarnings({ RAW_TYPES })
		private void invokeBatchListenerInTx(final ConsumerRecords<K, V> records,
				final List<ConsumerRecord<K, V>> recordList) {
			try {
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
				this.logger.error("Transaction rolled back", e);
				if (recordList == null) {
					getAfterRollbackProcessor().process(createRecordList(records), this.consumer, e, false);
				}
				else {
					getAfterRollbackProcessor().process(recordList, this.consumer, e, false);
				}
			}
		}

		private List<ConsumerRecord<K, V>> createRecordList(final ConsumerRecords<K, V> records) {
			List<ConsumerRecord<K, V>> recordList;
			recordList = new LinkedList<ConsumerRecord<K, V>>();
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext()) {
				recordList.add(iterator.next());
			}
			return recordList;
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
		private RuntimeException doInvokeBatchListener(final ConsumerRecords<K, V> records,
				List<ConsumerRecord<K, V>> recordList, @SuppressWarnings(RAW_TYPES) Producer producer) {

			try {
				invokeBatchOnMessage(records, recordList, producer);
			}
			catch (RuntimeException e) {
				if (this.containerProperties.isAckOnError() && !this.autoCommit && producer == null) {
					for (ConsumerRecord<K, V> record : getHighestOffsetRecords(records)) {
						this.acks.add(record);
					}
				}
				if (this.batchErrorHandler == null) {
					throw e;
				}
				try {
					invokeBatchErrorHandler(records, producer, e);
				}
				catch (RuntimeException ee) {
					this.logger.error("Error handler threw an exception", ee);
					return ee;
				}
				catch (Error er) { //NOSONAR
					this.logger.error("Error handler threw an error", er);
					throw er;
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return null;
		}

		private void invokeBatchOnMessage(final ConsumerRecords<K, V> records, List<ConsumerRecord<K, V>> recordList,
				@SuppressWarnings(RAW_TYPES) Producer producer) throws InterruptedException {
			if (this.wantsFullRecords) {
				this.batchListener.onMessage(records,
						this.isAnyManualAck
								? new ConsumerBatchAcknowledgment(records)
								: null, this.consumer);
			}
			else {
				doInvokeBatchOnMessage(records, recordList);
			}
			if (!this.isAnyManualAck && !this.autoCommit) {
				for (ConsumerRecord<K, V> record : getHighestOffsetRecords(records)) {
					this.acks.put(record);
				}
				if (producer != null) {
					sendOffsetsToTransaction(producer);
				}
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

		private void invokeBatchErrorHandler(final ConsumerRecords<K, V> records,
				@SuppressWarnings(RAW_TYPES) @Nullable Producer producer, RuntimeException e) {

			if (this.batchErrorHandler instanceof ContainerAwareBatchErrorHandler) {
				((ContainerAwareBatchErrorHandler) this.batchErrorHandler)
						.handle(e, records, this.consumer, KafkaMessageListenerContainer.this.container);
			}
			else {
				this.batchErrorHandler.handle(e, records, this.consumer);
			}
			// if the handler handled the error (no exception), go ahead and commit
			if (producer != null) {
				for (ConsumerRecord<K, V> record : getHighestOffsetRecords(records)) {
					this.acks.add(record);
				}
				sendOffsetsToTransaction(producer);
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
		@SuppressWarnings({ RAW_TYPES })
		private void invokeRecordListenerInTx(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext()) {
				final ConsumerRecord<K, V> record = iterator.next();
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Processing " + record);
				}
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
					this.logger.error("Transaction rolled back", e);
					List<ConsumerRecord<K, V>> unprocessed = new ArrayList<>();
					unprocessed.add(record);
					while (iterator.hasNext()) {
						unprocessed.add(iterator.next());
					}
					getAfterRollbackProcessor().process(unprocessed, this.consumer, e, true);
				}
				finally {
					TransactionSupport.clearTransactionIdSuffix();
				}
			}
		}

		private void doInvokeWithRecords(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext()) {
				final ConsumerRecord<K, V> record = iterator.next();
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Processing " + record);
				}
				doInvokeRecordListener(record, null, iterator);
			}
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
		private RuntimeException doInvokeRecordListener(final ConsumerRecord<K, V> record,
				@SuppressWarnings(RAW_TYPES) Producer producer,
				Iterator<ConsumerRecord<K, V>> iterator) {

			try {
				invokeOnMessage(record, producer);
			}
			catch (RuntimeException e) {
				if (this.containerProperties.isAckOnError() && !this.autoCommit && producer == null) {
					ackCurrent(record);
				}
				if (this.errorHandler == null) {
					throw e;
				}
				try {
					invokeErrorHandler(record, producer, iterator, e);
				}
				catch (RuntimeException ee) {
					this.logger.error("Error handler threw an exception", ee);
					return ee;
				}
				catch (Error er) { //NOSONAR
					this.logger.error("Error handler threw an error", er);
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
			ackCurrent(record, producer);
		}

		private void doInvokeOnMessage(final ConsumerRecord<K, V> record) {
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

		private void invokeErrorHandler(final ConsumerRecord<K, V> record,
				@SuppressWarnings(RAWTYPES) @Nullable Producer producer,
				Iterator<ConsumerRecord<K, V>> iterator, RuntimeException e) {

			if (this.errorHandler instanceof ContainerAwareErrorHandler) {
				if (producer == null) {
					processCommits();
				}
				List<ConsumerRecord<?, ?>> records = new ArrayList<>();
				records.add(record);
				while (iterator.hasNext()) {
					records.add(iterator.next());
				}
				((ContainerAwareErrorHandler) this.errorHandler).handle(e, records, this.consumer,
						KafkaMessageListenerContainer.this.container);
			}
			else {
				this.errorHandler.handle(e, record, this.consumer);
			}
			if (producer != null) {
				ackCurrent(record, producer);
			}
		}

		public void checkDeser(final ConsumerRecord<K, V> record, String headerName) {
			Header header = record.headers().lastHeader(headerName);
			if (header != null) {
				try {
					DeserializationException ex = (DeserializationException) new ObjectInputStream(
							new ByteArrayInputStream(header.value())).readObject();
					Headers headers = new RecordHeaders(Arrays.stream(record.headers().toArray())
							.filter(h -> !h.key()
									.startsWith(ErrorHandlingDeserializer2.KEY_DESERIALIZER_EXCEPTION_HEADER_PREFIX))
							.collect(Collectors.toList()));
					ex.setHeaders(headers);
					throw ex;
				}
				catch (IOException | ClassNotFoundException | ClassCastException e) {
					this.logger.error("Failed to deserialize a deserialization exception", e);
				}
			}
		}

		public void ackCurrent(final ConsumerRecord<K, V> record) {
			ackCurrent(record, null);
		}

		public void ackCurrent(final ConsumerRecord<K, V> record, @SuppressWarnings(RAW_TYPES)
				@Nullable Producer producer) {

			if (this.isRecordAck) {
				Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
						Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
								new OffsetAndMetadata(record.offset() + 1));
				if (producer == null) {
					this.commitLogger.log(() -> "Committing: " + offsetsToCommit);
					if (this.containerProperties.isSyncCommits()) {
						this.consumer.commitSync(offsetsToCommit);
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
					this.logger.error("Send offsets to transaction failed", e);
				}
			}
		}

		@SuppressWarnings({ "unchecked", RAW_TYPES })
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
					if (this.logger.isDebugEnabled() && isCountAck) {
						this.logger.debug("Committing in " + ackMode.name() + " because count "
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
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Committing in AckMode.TIME " +
							"because time elapsed exceeds configured limit of " +
							this.containerProperties.getAckTime());
				}
				commitIfNecessary();
				this.last = now;
			}
			else if (ackMode.equals(AckMode.COUNT_TIME) && elapsed) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Committing in AckMode.COUNT_TIME " +
							"because time elapsed exceeds configured limit of " +
							this.containerProperties.getAckTime());
				}
				commitIfNecessary();
				this.last = now;
				this.count = 0;
			}
		}

		private void processSeeks() {
			TopicPartitionInitialOffset offset = this.seeks.poll();
			while (offset != null) {
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Seek: " + offset);
				}
				try {
					SeekPosition position = offset.getPosition();
					if (position == null) {
						this.consumer.seek(offset.topicPartition(), offset.initialOffset());
					}
					else if (position.equals(SeekPosition.BEGINNING)) {
						this.consumer.seekToBeginning(Collections.singletonList(offset.topicPartition()));
					}
					else {
						this.consumer.seekToEnd(Collections.singletonList(offset.topicPartition()));
					}
				}
				catch (Exception e) {
					this.logger.error("Exception while seeking " + offset, e);
				}
				offset = this.seeks.poll();
			}
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
					.map(e -> e.getKey())
					.collect(Collectors.toSet());
			beginnings.forEach(k -> partitions.remove(k));
			Set<TopicPartition> ends = partitions.entrySet().stream()
					.filter(e -> SeekPosition.END.equals(e.getValue().seekPosition))
					.map(e -> e.getKey())
					.collect(Collectors.toSet());
			ends.forEach(k -> partitions.remove(k));
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
							this.consumer.seekToEnd(Arrays.asList(topicPartition));
						}
						newOffset = Math.max(0, this.consumer.position(topicPartition) + offset);
					}
					else if (metadata.relativeToCurrent) {
						newOffset = this.consumer.position(topicPartition) + offset;
					}

					try {
						this.consumer.seek(topicPartition, newOffset);
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Reset " + topicPartition + " to offset " + newOffset);
						}
					}
					catch (Exception e) {
						this.logger.error("Failed to set initial offset for " + topicPartition
								+ " at " + newOffset + ". Position is " + this.consumer.position(topicPartition), e);
					}
				}
			}
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
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Commit list: " + commits);
			}
			if (!commits.isEmpty()) {
				this.commitLogger.log(() -> "Committing: " + commits);
				try {
					if (this.containerProperties.isSyncCommits()) {
						this.consumer.commitSync(commits);
					}
					else {
						this.consumer.commitAsync(commits, this.commitCallback);
					}
				}
				catch (WakeupException e) {
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
			this.seeks.add(new TopicPartitionInitialOffset(topic, partition, offset));
		}

		@Override
		public void seekToBeginning(String topic, int partition) {
			this.seeks.add(new TopicPartitionInitialOffset(topic, partition, SeekPosition.BEGINNING));
		}

		@Override
		public void seekToEnd(String topic, int partition) {
			this.seeks.add(new TopicPartitionInitialOffset(topic, partition, SeekPosition.END));
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
						this.logger.error("Failed to close producer with transaction id suffix: "
								+ zombieFenceTxIdSuffix(tp.topic(), tp.partition()), e);
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
				Assert.state(ListenerConsumer.this.isAnyManualAck,
						"A manual ackmode is required for an acknowledging listener");
				processAck(this.record);
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
				Assert.state(ListenerConsumer.this.isAnyManualAck,
						"A manual ackmode is required for an acknowledging listener");
				for (ConsumerRecord<K, V> record : getHighestOffsetRecords(this.records)) {
					processAck(record);
				}
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
				super();
			}

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
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
				if (ListenerConsumer.this.kafkaTxManager != null) {
					closeProducers(partitions);
				}
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				if (ListenerConsumer.this.consumerPaused) {
					ListenerConsumer.this.consumerPaused = false;
					ListenerConsumer.this.logger.warn("Paused consumer resumed by Kafka due to rebalance; "
							+ "the container will pause again before polling, unless the container's "
							+ "'paused' property is reset by a custom rebalance listener");
				}
				ListenerConsumer.this.assignedPartitions = partitions;
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
							ListenerConsumer.this.logger.error("No offset and no reset policy", e);
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

									@SuppressWarnings({ "unchecked", RAWTYPES })
									@Override
									protected void doInTransactionWithoutResult(TransactionStatus status) {
										KafkaResourceHolder holder =
											(KafkaResourceHolder) TransactionSynchronizationManager
												.getResource(
													ListenerConsumer.this.kafkaTxManager.getProducerFactory());
										if (holder != null) {
											holder.getProducer().sendOffsetsToTransaction(
													Collections.singletonMap(partition, offsetAndMetadata),
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
					else if (KafkaMessageListenerContainer.this.getContainerProperties().isSyncCommits()) {
						ListenerConsumer.this.consumer.commitSync(offsetsToCommit);
					}
					else {
						ListenerConsumer.this.consumer.commitAsync(offsetsToCommit,
								KafkaMessageListenerContainer.this.getContainerProperties().getCommitCallback());
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

		}

	}

	private static final class LoggingCommitCallback implements OffsetCommitCallback {

		private static final Log logger = LogFactory.getLog(LoggingCommitCallback.class); // NOSONAR

		LoggingCommitCallback() {
			super();
		}

		@Override
		public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
			if (exception != null) {
				logger.error("Commit failed for " + offsets, exception);
			}
			else if (logger.isDebugEnabled()) {
				logger.debug("Commits for " + offsets + " completed");
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
			KafkaMessageListenerContainer.this.logger.error("Error while stopping the container: ", e);
			if (this.callback != null) {
				this.callback.run();
			}
		}

		@Override
		public void onSuccess(Object result) {
			if (KafkaMessageListenerContainer.this.logger.isDebugEnabled()) {
				KafkaMessageListenerContainer.this.logger
						.debug(KafkaMessageListenerContainer.this + " stopped normally");
			}
			if (this.callback != null) {
				this.callback.run();
			}
		}

	}

}
