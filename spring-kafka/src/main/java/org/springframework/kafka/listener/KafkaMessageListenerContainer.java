/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
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
 */
public class KafkaMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private final ConsumerFactory<K, V> consumerFactory;

	private final TopicPartitionInitialOffset[] topicPartitions;

	private ListenerConsumer listenerConsumer;

	private ListenableFuture<?> listenerConsumerFuture;

	private MessageListener<K, V> listener;

	private AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

	private String clientIdSuffix;

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties) {
		this(consumerFactory, containerProperties, (TopicPartitionInitialOffset[]) null);
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
		super(containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.consumerFactory = consumerFactory;
		if (topicPartitions != null) {
			this.topicPartitions = Arrays.copyOf(topicPartitions, topicPartitions.length);
		}
		else {
			this.topicPartitions = containerProperties.getTopicPartitions();
		}
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
	public Collection<TopicPartition> getAssignedPartitions() {
		if (this.listenerConsumer.definedPartitions != null) {
			return Collections.unmodifiableCollection(this.listenerConsumer.definedPartitions.keySet());
		}
		else if (this.listenerConsumer.assignedPartitions != null) {
			return Collections.unmodifiableCollection(this.listenerConsumer.assignedPartitions);
		}
		else {
			return null;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doStart() {
		if (isRunning()) {
			return;
		}
		ContainerProperties containerProperties = getContainerProperties();

		if (!this.consumerFactory.isAutoCommit()) {
			AckMode ackMode = containerProperties.getAckMode();
			if (ackMode.equals(AckMode.COUNT) || ackMode.equals(AckMode.COUNT_TIME)) {
				Assert.state(containerProperties.getAckCount() > 0, "'ackCount' must be > 0");
			}
			if ((ackMode.equals(AckMode.TIME) || ackMode.equals(AckMode.COUNT_TIME))
					&& containerProperties.getAckTime() == 0) {
				containerProperties.setAckTime(5000);
			}
		}

		Object messageListener = containerProperties.getMessageListener();
		Assert.state(messageListener != null, "A MessageListener is required");
		if (messageListener instanceof AcknowledgingMessageListener) {
			this.acknowledgingMessageListener = (AcknowledgingMessageListener<K, V>) messageListener;
		}
		else if (messageListener instanceof MessageListener) {
			this.listener = (MessageListener<K, V>) messageListener;
		}
		else {
			throw new IllegalStateException("messageListener must be 'MessageListener' "
					+ "or 'AcknowledgingMessageListener', not " + messageListener.getClass().getName());
		}
		if (containerProperties.getConsumerTaskExecutor() == null) {
			SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-kafka-consumer-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}
		if (containerProperties.getListenerTaskExecutor() == null) {
			SimpleAsyncTaskExecutor listenerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-kafka-listener-");
			containerProperties.setListenerTaskExecutor(listenerExecutor);
		}
		this.listenerConsumer = new ListenerConsumer(this.listener, this.acknowledgingMessageListener);
		setRunning(true);
		this.listenerConsumerFuture = containerProperties
				.getConsumerTaskExecutor()
				.submitListenable(this.listenerConsumer);
	}

	@Override
	protected void doStop(final Runnable callback) {
		if (isRunning()) {
			this.listenerConsumerFuture.addCallback(new ListenableFutureCallback<Object>() {
				@Override
				public void onFailure(Throwable e) {
					KafkaMessageListenerContainer.this.logger.error("Error while stopping the container: ", e);
					if (callback != null) {
						callback.run();
					}
				}

				@Override
				public void onSuccess(Object result) {
					if (KafkaMessageListenerContainer.this.logger.isDebugEnabled()) {
						KafkaMessageListenerContainer.this.logger
								.debug(KafkaMessageListenerContainer.this + " stopped normally");
					}
					if (callback != null) {
						callback.run();
					}
				}
			});
			setRunning(false);
			this.listenerConsumer.consumer.wakeup();
		}
	}


	@Override
	public String toString() {
		return "KafkaMessageListenerContainer [id=" + getBeanName()
				+ (this.clientIdSuffix != null ? ", clientIndex=" + this.clientIdSuffix : "")
				+ ", topicPartitions=" + getAssignedPartitions()
				+ "]";
	}


	private final class ListenerConsumer implements SchedulingAwareRunnable {

		private final Log logger = LogFactory.getLog(ListenerConsumer.class);

		private final ContainerProperties containerProperties = getContainerProperties();

		private final OffsetCommitCallback commitCallback = this.containerProperties.getCommitCallback() != null
				? this.containerProperties.getCommitCallback()
				: new LoggingCommitCallback();

		private final Consumer<K, V> consumer;

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final MessageListener<K, V> listener;

		private final AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

		private final boolean autoCommit = KafkaMessageListenerContainer.this.consumerFactory.isAutoCommit();

		private final boolean isManualAck = this.containerProperties.getAckMode().equals(AckMode.MANUAL);

		private final boolean isManualImmediateAck =
				this.containerProperties.getAckMode().equals(AckMode.MANUAL_IMMEDIATE);

		private final boolean isAnyManualAck = this.isManualAck || this.isManualImmediateAck;

		private final boolean isRecordAck = this.containerProperties.getAckMode().equals(AckMode.RECORD);

		private final boolean isBatchAck = this.containerProperties.getAckMode().equals(AckMode.BATCH);

		private final BlockingQueue<ConsumerRecords<K, V>> recordsToProcess =
				new LinkedBlockingQueue<>(this.containerProperties.getQueueDepth());

		private final BlockingQueue<ConsumerRecord<K, V>> acks = new LinkedBlockingQueue<>();

		private final ApplicationEventPublisher applicationEventPublisher = getApplicationEventPublisher();

		private volatile Map<TopicPartition, Long> definedPartitions;

		private ConsumerRecords<K, V> unsent;

		private volatile Collection<TopicPartition> assignedPartitions;

		private int count;

		private volatile ListenerInvoker invoker;

		private long last;

		private volatile Future<?> listenerInvokerFuture;

		/**
		 * The consumer is currently paused due to a slow listener. The consumer will be
		 * resumed when the current batch of records has been processed but will continue
		 * to be polled.
		 */
		private boolean paused;

		@SuppressWarnings({"deprecation", "unchecked"})
		ListenerConsumer(MessageListener<K, V> listener, AcknowledgingMessageListener<K, V> ackListener) {
			Assert.state(!this.isAnyManualAck || !this.autoCommit,
				"Consumer cannot be configured for auto commit for ackMode " + this.containerProperties.getAckMode());
			final Consumer<K, V> consumer =
					KafkaMessageListenerContainer.this.consumerFactory
							instanceof org.springframework.kafka.core.ClientIdSuffixAware
							? ((org.springframework.kafka.core.ClientIdSuffixAware<K, V>) KafkaMessageListenerContainer
									.this.consumerFactory)
										.createConsumer(KafkaMessageListenerContainer.this.clientIdSuffix)
							: KafkaMessageListenerContainer.this.consumerFactory.createConsumer();

			ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					// do not stop the invoker if it is not started yet
					// this will occur on the initial start on a subscription
					if (!ListenerConsumer.this.autoCommit) {
						if (ListenerConsumer.this.logger.isTraceEnabled()) {
							ListenerConsumer.this.logger.trace("Received partition revocation notification, " +
									"and will stop the invoker.");
						}
						if (ListenerConsumer.this.listenerInvokerFuture != null) {
							stopInvokerAndCommitManualAcks();
							ListenerConsumer.this.recordsToProcess.clear();
							ListenerConsumer.this.unsent = null;
						}
						else {
							if (!CollectionUtils.isEmpty(partitions)) {
								ListenerConsumer.this.logger.error("Invalid state: the invoker was not active, " +
												"but the consumer had allocated partitions");
							}
						}
					}
					else {
						if (ListenerConsumer.this.logger.isTraceEnabled()) {
							ListenerConsumer.this.logger.trace("Received partition revocation notification, " +
									"but the container is in autocommit mode, " +
									"so transition will be handled by the consumer");
						}
					}
					getContainerProperties().getConsumerRebalanceListener().onPartitionsRevoked(partitions);
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					ListenerConsumer.this.assignedPartitions = partitions;
					if (!ListenerConsumer.this.autoCommit) {
						// Commit initial positions - this is generally redundant but
						// it protects us from the case when another consumer starts
						// and rebalance would cause it to reset at the end
						// see https://github.com/spring-projects/spring-kafka/issues/110
						Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
						for (TopicPartition partition : partitions) {
							offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
						}
						if (ListenerConsumer.this.logger.isDebugEnabled()) {
							ListenerConsumer.this.logger.debug("Committing: " + offsets);
						}
						if (KafkaMessageListenerContainer.this.getContainerProperties().isSyncCommits()) {
							ListenerConsumer.this.consumer.commitSync(offsets);
						}
						else {
							ListenerConsumer.this.consumer.commitAsync(offsets,
									KafkaMessageListenerContainer.this.getContainerProperties().getCommitCallback());
						}
					}
					// We will not start the invoker thread if we are in autocommit mode,
					// as we will execute synchronously then
					// We will not start the invoker thread if the container is stopped
					// We will not start the invoker thread if there are no partitions to
					// listen to
					if (!ListenerConsumer.this.autoCommit && KafkaMessageListenerContainer.this.isRunning()
							&& !CollectionUtils.isEmpty(partitions)) {
						startInvoker();
					}
					getContainerProperties().getConsumerRebalanceListener().onPartitionsAssigned(partitions);
				}

			};

			if (KafkaMessageListenerContainer.this.topicPartitions == null) {
				if (this.containerProperties.getTopicPattern() != null) {
					consumer.subscribe(this.containerProperties.getTopicPattern(), rebalanceListener);
				}
				else {
					consumer.subscribe(Arrays.asList(this.containerProperties.getTopics()), rebalanceListener);
				}
			}
			else {
				List<TopicPartitionInitialOffset> topicPartitions =
						Arrays.asList(KafkaMessageListenerContainer.this.topicPartitions);
				this.definedPartitions = new HashMap<>(topicPartitions.size());
				for (TopicPartitionInitialOffset topicPartition : topicPartitions) {
					this.definedPartitions.put(topicPartition.topicPartition(), topicPartition.initialOffset());
				}
				consumer.assign(new ArrayList<>(this.definedPartitions.keySet()));
			}
			this.consumer = consumer;
			this.listener = listener;
			this.acknowledgingMessageListener = ackListener;
		}

		private void startInvoker() {
			ListenerConsumer.this.invoker = new ListenerInvoker();
			ListenerConsumer.this.listenerInvokerFuture = this.containerProperties.getListenerTaskExecutor()
					.submit(ListenerConsumer.this.invoker);
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			this.count = 0;
			this.last = System.currentTimeMillis();
			if (isRunning() && this.definedPartitions != null) {
				initPartitionsIfNeeded();
				// we start the invoker here as there will be no rebalance calls to
				// trigger it, but only if the container is not set to autocommit
				// otherwise we will process records on a separate thread
				if (!this.autoCommit) {
					startInvoker();
				}
			}
			long lastReceive = System.currentTimeMillis();
			long lastAlertAt = lastReceive;
			while (isRunning()) {
				try {
					if (!this.autoCommit) {
						processCommits();
					}
					if (this.logger.isTraceEnabled()) {
						this.logger.trace("Polling (paused=" + this.paused + ")...");
					}
					ConsumerRecords<K, V> records = this.consumer.poll(this.containerProperties.getPollTimeout());
					if (records != null && this.logger.isDebugEnabled()) {
						this.logger.debug("Received: " + records.count() + " records");
					}
					if (records != null && records.count() > 0) {
						if (this.containerProperties.getIdleEventInterval() != null) {
							lastReceive = System.currentTimeMillis();
						}
						// if the container is set to auto-commit, then execute in the
						// same thread
						// otherwise send to the buffering queue
						if (this.autoCommit) {
							invokeListener(records);
						}
						else {
							if (sendToListener(records)) {
								if (this.assignedPartitions != null) {
									// avoid group management rebalance due to a slow
									// consumer
									this.consumer.pause(this.assignedPartitions
											.toArray(new TopicPartition[this.assignedPartitions.size()]));
									this.paused = true;
									this.unsent = records;
								}
							}
						}
					}
					else {
						if (this.containerProperties.getIdleEventInterval() != null) {
							long now = System.currentTimeMillis();
							if (now > lastReceive + this.containerProperties.getIdleEventInterval()
									&& now > lastAlertAt + this.containerProperties.getIdleEventInterval()) {
								publishIdleContainerEvent(now - lastReceive);
								lastAlertAt = now;
							}
						}
					}
					this.unsent = checkPause(this.unsent);
				}
				catch (WakeupException e) {
					this.unsent = checkPause(this.unsent);
				}
				catch (Exception e) {
					if (this.containerProperties.getErrorHandler() != null) {
						this.containerProperties.getErrorHandler().handle(e, null);
					}
					else {
						this.logger.error("Container exception", e);
					}
				}
			}
			if (this.listenerInvokerFuture != null) {
				stopInvokerAndCommitManualAcks();
			}
			try {
				this.consumer.unsubscribe();
			}
			catch (WakeupException e) {
				// No-op. Continue process
			}
			this.consumer.close();
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Consumer stopped");
			}
		}

		private void publishIdleContainerEvent(long idleTime) {
			if (this.applicationEventPublisher != null) {
				this.applicationEventPublisher.publishEvent(new ListenerContainerIdleEvent(
						KafkaMessageListenerContainer.this, idleTime, getBeanName(), getAssignedPartitions()));
			}
		}

		private void stopInvokerAndCommitManualAcks() {
			long now = System.currentTimeMillis();
			this.invoker.stop();
			long remaining = this.containerProperties.getShutdownTimeout() + now - System.currentTimeMillis();
			try {
				this.listenerInvokerFuture.get(remaining, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (ExecutionException e) {
				this.logger.error("Error while shutting down the listener invoker:", e);
			}
			catch (TimeoutException e) {
				this.logger.info("Invoker timed out while waiting for shutdown and will be canceled.");
				this.listenerInvokerFuture.cancel(true);
			}
			finally {
				this.listenerInvokerFuture = null;
			}
			processCommits();
			if (this.offsets.size() > 0) {
				// we always commit after stopping the invoker
				commitIfNecessary();
			}
			this.invoker = null;
		}

		private ConsumerRecords<K, V> checkPause(ConsumerRecords<K, V> unsent) {
			if (this.paused && this.recordsToProcess.size() < this.containerProperties.getQueueDepth()) {
				// Listener has caught up.
				this.consumer.resume(
						this.assignedPartitions.toArray(new TopicPartition[this.assignedPartitions.size()]));
				this.paused = false;
				if (unsent != null) {
					try {
						sendToListener(unsent);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new KafkaException("Interrupted while sending to listener", e);
					}
				}
				return null;
			}
			return unsent;
		}

		private boolean sendToListener(final ConsumerRecords<K, V> records) throws InterruptedException {
			if (this.containerProperties.isPauseEnabled() && CollectionUtils.isEmpty(this.definedPartitions)) {
				return !this.recordsToProcess.offer(records, this.containerProperties.getPauseAfter(),
						TimeUnit.MILLISECONDS);
			}
			else {
				this.recordsToProcess.put(records);
				return false;
			}
		}

		/**
		 * Process any acks that have been queued by the listener thread.
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
			if (ListenerConsumer.this.isManualImmediateAck) {
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

		private void ackImmediate(ConsumerRecord<K, V> record) {
			Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
					new TopicPartition(record.topic(), record.partition()),
					new OffsetAndMetadata(record.offset() + 1));
			if (ListenerConsumer.this.logger.isDebugEnabled()) {
				ListenerConsumer.this.logger.debug("Committing: " + commits);
			}
			if (this.containerProperties.isSyncCommits()) {
				ListenerConsumer.this.consumer.commitSync(commits);
			}
			else {
				ListenerConsumer.this.consumer.commitAsync(commits,
						ListenerConsumer.this.commitCallback);
			}
		}

		private void invokeListener(final ConsumerRecords<K, V> records) {
			Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
			while (iterator.hasNext() && (this.autoCommit || (this.invoker != null && this.invoker.active))) {
				final ConsumerRecord<K, V> record = iterator.next();
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Processing " + record);
				}
				try {
					if (this.acknowledgingMessageListener != null) {
						this.acknowledgingMessageListener.onMessage(record, this.isAnyManualAck
								? new ConsumerAcknowledgment(record, this.isManualImmediateAck) : null);
					}
					else {
						this.listener.onMessage(record);
					}
					if (!this.isAnyManualAck && !this.autoCommit) {
						this.acks.add(record);
					}
				}
				catch (Exception e) {
					if (this.containerProperties.isAckOnError() && !this.autoCommit) {
						this.acks.add(record);
					}
					if (this.containerProperties.getErrorHandler() != null) {
						this.containerProperties.getErrorHandler().handle(e, record);
					}
					else {
						this.logger.error("Listener threw an exception and no error handler for " + record, e);
					}
				}
			}
		}

		private void processCommits() {
			handleAcks();
			this.count += this.acks.size();
			long now;
			AckMode ackMode = this.containerProperties.getAckMode();
			if (!this.isManualImmediateAck) {
				if (!this.isManualAck) {
					updatePendingOffsets();
				}
				boolean countExceeded = this.count >= this.containerProperties.getAckCount();
				if (this.isManualAck || this.isBatchAck || this.isRecordAck
						|| (ackMode.equals(AckMode.COUNT) && countExceeded)) {
					if (this.logger.isDebugEnabled() && ackMode.equals(AckMode.COUNT)) {
						this.logger.debug("Committing in AckMode.COUNT because count " + this.count
								+ " exceeds configured limit of " + this.containerProperties.getAckCount());
					}
					commitIfNecessary();
					this.count = 0;
				}
				else {
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
					else if (ackMode.equals(AckMode.COUNT_TIME) && (elapsed || countExceeded)) {
						if (this.logger.isDebugEnabled()) {
							if (elapsed) {
								this.logger.debug("Committing in AckMode.COUNT_TIME " +
										"because time elapsed exceeds configured limit of " +
										this.containerProperties.getAckTime());
							}
							else {
								this.logger.debug("Committing in AckMode.COUNT_TIME " +
										"because count " + this.count + " exceeds configured limit of" +
										this.containerProperties.getAckCount());
							}
						}

						commitIfNecessary();
						this.last = now;
						this.count = 0;
					}
				}
			}
		}

		private void initPartitionsIfNeeded() {
			/*
			 * Note: initial position setting is only supported with explicit topic assignment.
			 * When using auto assignment (subscribe), the ConsumerRebalanceListener is not
			 * called until we poll() the consumer.
			 */
			for (Entry<TopicPartition, Long> entry : this.definedPartitions.entrySet()) {
				TopicPartition topicPartition = entry.getKey();
				Long offset = entry.getValue();
				if (offset != null) {
					long newOffset = offset;

					if (offset < 0) {
						this.consumer.seekToEnd(topicPartition);
						newOffset = Math.max(0, this.consumer.position(topicPartition) + offset);
					}

					try {
						this.consumer.seek(topicPartition, newOffset);
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Reset " + topicPartition + " to offset " + newOffset);
						}
					}
					catch (Exception e) {
						logger.error("Failed to set initial offset for " + topicPartition
								+ " at " + newOffset + ". Positioned to " + this.consumer.position(topicPartition), e);
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
			if (!this.offsets.containsKey(record.topic())) {
				this.offsets.put(record.topic(), new HashMap<Integer, Long>());
			}
			this.offsets.get(record.topic()).put(record.partition(), record.offset());
		}

		private void commitIfNecessary() {
			Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
			for (Entry<String, Map<Integer, Long>> entry : this.offsets.entrySet()) {
				for (Entry<Integer, Long> offset : entry.getValue().entrySet()) {
					commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
							new OffsetAndMetadata(offset.getValue() + 1));
				}
			}
			this.offsets.clear();
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Commit list: " + commits);
			}
			if (!commits.isEmpty()) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Committing: " + commits);
				}
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
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Woken up during commit");
					}
				}
			}
		}

		private final class ListenerInvoker implements SchedulingAwareRunnable {

			private final CountDownLatch exitLatch = new CountDownLatch(1);

			private volatile boolean active = true;

			private volatile Thread executingThread;

			ListenerInvoker() {
				super();
			}

			@Override
			public void run() {
				Assert.isTrue(this.active, "This instance is not active anymore");
				try {
					this.executingThread = Thread.currentThread();
					while (this.active) {
						try {
							ConsumerRecords<K, V> records = ListenerConsumer.this.recordsToProcess.poll(1,
									TimeUnit.SECONDS);
							if (this.active) {
								if (records != null) {
									invokeListener(records);
								}
								else {
									if (ListenerConsumer.this.logger.isTraceEnabled()) {
										ListenerConsumer.this.logger.trace("No records to process");
									}
								}
							}
						}
						catch (InterruptedException e) {
							if (!this.active) {
								Thread.currentThread().interrupt();
							}
							else {
								ListenerConsumer.this.logger.debug("Interrupt ignored");
							}
						}
					}
				}
				finally {
					this.active = false;
					this.exitLatch.countDown();
				}
			}

			@Override
			public boolean isLongLived() {
				return true;
			}

			private void stop() {
				if (ListenerConsumer.this.logger.isDebugEnabled()) {
					ListenerConsumer.this.logger.debug("Stopping invoker");
				}
				this.active = false;
				try {
					if (!this.exitLatch.await(getContainerProperties().getShutdownTimeout(), TimeUnit.MILLISECONDS)
							&& this.executingThread != null) {
						if (ListenerConsumer.this.logger.isDebugEnabled()) {
							ListenerConsumer.this.logger.debug("Interrupting invoker");
						}
						this.executingThread.interrupt();
					}
				}
				catch (InterruptedException e) {
					if (this.executingThread != null) {
						this.executingThread.interrupt();
					}
					Thread.currentThread().interrupt();
				}
				if (ListenerConsumer.this.logger.isDebugEnabled()) {
					ListenerConsumer.this.logger.debug("Invoker stopped");
				}
			}
		}

		private final class ConsumerAcknowledgment implements Acknowledgment {

			private final ConsumerRecord<K, V> record;

			private final boolean immediate;

			ConsumerAcknowledgment(ConsumerRecord<K, V> record, boolean immediate) {
				this.record = record;
				this.immediate = immediate;
			}

			@Override
			public void acknowledge() {
				try {
					if (ListenerConsumer.this.autoCommit) {
						throw new IllegalStateException("Manual acks are not allowed when auto commit is used");
					}
					ListenerConsumer.this.acks.put(this.record);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new KafkaException("Interrupted while queuing ack for " + this.record, e);
				}
				if (this.immediate) {
					ListenerConsumer.this.consumer.wakeup();
				}
			}

			@Override
			public String toString() {
				return "Acknowledgment for " + this.record;
			}

		}

	}

	private static final class LoggingCommitCallback implements OffsetCommitCallback {

		private static final Log logger = LogFactory.getLog(LoggingCommitCallback.class);

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

}
