/*
 * Copyright 2016 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

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

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;

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
 */
public class KafkaMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private final ConsumerFactory<K, V> consumerFactory;

	private final String[] topics;

	private final Pattern topicPattern;

	private final TopicPartition[] partitions;

	private final ConsumerRebalanceListener consumerRebalanceListener;

	private ListenerConsumer listenerConsumer;

	private long recentOffset;

	private MessageListener<K, V> listener;

	private AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

	private OffsetCommitCallback commitCallback;

	private boolean syncCommits = true;

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions - when using this constructor, {@link #setRecentOffset(long)
	 * recentOffset} can be specified.
	 * @param consumerFactory the consumer factory.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, TopicPartition... topicPartitions) {
		this(consumerFactory, null, null, null, topicPartitions);
	}

	/**
	 * Construct an instance with the supplied configuration properties,
	 * {@link ConsumerRebalanceListener} implementation and specific topics/partitions -
	 * when using this constructor, {@link #setRecentOffset(long) recentOffset} can be specified.
	 * @param consumerFactory the consumer factory.
	 * @param consumerRebalanceListener {@link ConsumerRebalanceListener} implementation.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ConsumerRebalanceListener consumerRebalanceListener, TopicPartition... topicPartitions) {
		this(consumerFactory, consumerRebalanceListener, null, null, topicPartitions);
	}

	/**
	 * Construct an instance with the supplied configuration properties and topics.
	 * When using this constructor, {@link #setRecentOffset(long) recentOffset} is
	 * ignored.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, String... topics) {
		this(consumerFactory, null, topics, null, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties,
	 * {@link ConsumerRebalanceListener} implementation and topics. When using this constructor,
	 * {@link #setRecentOffset(long) recentOffset} is ignored.
	 * @param consumerFactory the consumer factory.
	 * @param consumerRebalanceListener {@link ConsumerRebalanceListener} implementation.
	 * @param topics the topics.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ConsumerRebalanceListener consumerRebalanceListener, String... topics) {
		this(consumerFactory, consumerRebalanceListener, topics, null, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and topic
	 * pattern. When using this constructor, {@link #setRecentOffset(long) recentOffset} is
	 * ignored.
	 * @param consumerFactory the consumer factory.
	 * @param topicPattern the topic pattern.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, Pattern topicPattern) {
		this(consumerFactory, null, null, topicPattern, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties,
	 * {@link ConsumerRebalanceListener} implementation and topic pattern.
	 * When using this constructor, {@link #setRecentOffset(long) recentOffset} is ignored.
	 * @param consumerFactory the consumer factory.
	 * @param consumerRebalanceListener {@link ConsumerRebalanceListener} implementation.
	 * @param topicPattern the topic pattern.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ConsumerRebalanceListener consumerRebalanceListener, Pattern topicPattern) {
		this(consumerFactory, consumerRebalanceListener, null, topicPattern, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and topic
	 * pattern. Note: package protected - used by the ConcurrentMessageListenerContainer.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 * @param topicPattern the topic pattern.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, String[] topics, Pattern topicPattern,
			TopicPartition[] topicPartitions) {
		this(consumerFactory, null, null, topicPattern, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties,
	 * {@link ConsumerRebalanceListener} implementation
	 * and topic pattern. Note: package protected - used by the ConcurrentMessageListenerContainer.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 * @param topicPattern the topic pattern.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 * @param consumerRebalanceListener the {@link ConsumerRebalanceListener} instance
	 */
	KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ConsumerRebalanceListener consumerRebalanceListener,
			String[] topics, Pattern topicPattern, TopicPartition[] topicPartitions) {
		this.consumerFactory = consumerFactory;
		this.consumerRebalanceListener = (consumerRebalanceListener == null) ? createConsumerRebalanceListener() :
				consumerRebalanceListener;
		this.topics = topics == null ? null : Arrays.asList(topics).toArray(new String[topics.length]);
		this.topicPattern = topicPattern;
		this.partitions = topicPartitions == null ? null : new LinkedHashSet<>(Arrays.asList(topicPartitions))
				.toArray(new TopicPartition[topicPartitions.length]);
	}

	/**
	 * Set the offset to this number of records back from the latest when starting.
	 * Overrides any consumer properties (earliest, latest).
	 * Only applies when explicit topic/partition assignment is provided.
	 * @param recentOffset the offset from the latest; default 0.
	 */
	public void setRecentOffset(long recentOffset) {
		this.recentOffset = recentOffset;
	}

	/**
	 * Set the commit callback; by default a simple logging callback is used to
	 * log success at DEBUG level and failures at ERROR level.
	 * @param commitCallback the callback.
	 */
	public void setCommitCallback(OffsetCommitCallback commitCallback) {
		this.commitCallback = commitCallback;
	}

	/**
	 * Set whether or not to call consumer.commitSync() or commitAsync() when
	 * the container is responsible for commits. Default true. See
	 * https://github.com/spring-projects/spring-kafka/issues/62
	 * At the time of writing, async commits are not entirely reliable.
	 * @param syncCommits true to use commitSync().
	 */
	public void setSyncCommits(boolean syncCommits) {
		this.syncCommits = syncCommits;
	}

	/**
	 * Return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by Kafka; may be null if not assigned yet.
	 * @return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by Kafka; may be null if not assigned yet.
	 */
	public Collection<TopicPartition> getAssignedPartitions() {
		if (this.listenerConsumer.definedPartitions != null) {
			return Collections.unmodifiableCollection(this.listenerConsumer.definedPartitions);
		}
		else if (this.listenerConsumer.assignedPartitions != null) {
			return Collections.unmodifiableCollection(this.listenerConsumer.assignedPartitions);
		}
		else {
			return null;
		}
	}

	/**
	 * Return default implementation of {@link ConsumerRebalanceListener} instance.
	 * @return the {@link ConsumerRebalanceListener} currently assigned to this container.
	 */
	protected final ConsumerRebalanceListener createConsumerRebalanceListener() {
		return new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				logger.info("partitions revoked:" + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("partitions assigned:" + partitions);
			}

		};
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void doStart() {
		if (isRunning()) {
			return;
		}
		setRunning(true);
		Object messageListener = getMessageListener();
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
		if (getTaskExecutor() == null) {
			setTaskExecutor(
					new SimpleAsyncTaskExecutor(getBeanName() == null ? "kafka-" : (getBeanName() + "-kafka-")));
		}
		this.listenerConsumer = new ListenerConsumer(this.listener, this.acknowledgingMessageListener,
				this.recentOffset);
		getTaskExecutor().execute(this.listenerConsumer);
	}

	@Override
	protected void doStop() {
		if (isRunning()) {
			setRunning(false);
			this.listenerConsumer.consumer.wakeup();
		}
	}


	private class ListenerConsumer implements SchedulingAwareRunnable {

		private final Log logger = LogFactory.getLog(ListenerConsumer.class);

		private final OffsetCommitCallback commitCallback = KafkaMessageListenerContainer.this.commitCallback != null
				? KafkaMessageListenerContainer.this.commitCallback
				: new LoggingCommitCallback();

		private final Consumer<K, V> consumer;

		private final ConcurrentMap<String, ConcurrentMap<Integer, Long>> manualOffsets = new ConcurrentHashMap<>();

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final MessageListener<K, V> listener;

		private final AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

		private final long recentOffset;

		private final boolean autoCommit = KafkaMessageListenerContainer.this.consumerFactory.isAutoCommit();

		private final AckMode ackMode = getAckMode();

		private final boolean syncCommits = KafkaMessageListenerContainer.this.syncCommits;

		private Thread consumerThread;

		private volatile Collection<TopicPartition> definedPartitions;

		private volatile Collection<TopicPartition> assignedPartitions;

		private int count;

		private long last;

		ListenerConsumer(MessageListener<K, V> listener, AcknowledgingMessageListener<K, V> ackListener,
				long recentOffset) {
			Assert.state(!(this.ackMode.equals(AckMode.MANUAL)
						|| this.ackMode.equals(AckMode.MANUAL_IMMEDIATE)
						|| this.ackMode.equals(AckMode.MANUAL_IMMEDIATE_SYNC))
					|| !this.autoCommit,
					"Consumer cannot be configured for auto commit for ackMode " + this.ackMode);
			Consumer<K, V> consumer = KafkaMessageListenerContainer.this.consumerFactory.createConsumer();

			ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					KafkaMessageListenerContainer.this.consumerRebalanceListener.onPartitionsRevoked(partitions);
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					ListenerConsumer.this.assignedPartitions = partitions;
					KafkaMessageListenerContainer.this.consumerRebalanceListener.onPartitionsAssigned(partitions);
				}

			};

			if (KafkaMessageListenerContainer.this.partitions == null) {
				if (KafkaMessageListenerContainer.this.topicPattern != null) {
					consumer.subscribe(KafkaMessageListenerContainer.this.topicPattern, rebalanceListener);
				}
				else {
					consumer.subscribe(Arrays.asList(KafkaMessageListenerContainer.this.topics), rebalanceListener);
				}
			}
			else {
				List<TopicPartition> topicPartitions = Arrays.asList(KafkaMessageListenerContainer.this.partitions);
				this.definedPartitions = topicPartitions;
				consumer.assign(topicPartitions);
			}
			this.consumer = consumer;
			this.listener = listener;
			this.acknowledgingMessageListener = ackListener;
			this.recentOffset = recentOffset;
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			this.consumerThread = Thread.currentThread();
			this.count = 0;
			this.last = System.currentTimeMillis();
			if (isRunning() && this.definedPartitions != null) {
				initPartitionsIfNeeded();
			}
			while (isRunning()) {
				try {
					if (this.logger.isTraceEnabled()) {
						this.logger.trace("Polling...");
					}
					ConsumerRecords<K, V> records = this.consumer.poll(getPollTimeout());
					if (records != null) {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Received: " + records.count() + " records");
						}
						Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
						while (iterator.hasNext()) {
							final ConsumerRecord<K, V> record = iterator.next();
							invokeListener(record);
							if (!this.autoCommit && this.ackMode.equals(AckMode.RECORD)) {
								this.consumer.commitAsync(
										Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
												new OffsetAndMetadata(record.offset() + 1)), this.commitCallback);
							}
						}
						if (!this.autoCommit) {
							processCommits(this.ackMode, records);
						}
					}
					else {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("No records");
						}
					}
				}
				catch (WakeupException e) {
					// No-op. Continue process
				}
				catch (Exception e) {
					if (getErrorHandler() != null) {
						getErrorHandler().handle(e, null);
					}
					else {
						this.logger.error("Container exception", e);
					}
				}
			}
			if (this.offsets.size() > 0) {
				commitIfNecessary();
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

		private void invokeListener(final ConsumerRecord<K, V> record) {
			try {
				if (this.acknowledgingMessageListener != null) {
					this.acknowledgingMessageListener.onMessage(record, new Acknowledgment() { //NOSONAR - long anon.

						@Override
						public void acknowledge() {
							if (ListenerConsumer.this.ackMode.equals(AckMode.MANUAL)) {
								updateManualOffset(record);
							}
							else if (ListenerConsumer.this.ackMode.equals(AckMode.MANUAL_IMMEDIATE)
									|| ListenerConsumer.this.ackMode.equals(AckMode.MANUAL_IMMEDIATE_SYNC)) {
								ackImmediate(record);
							}
							else {
								throw new IllegalStateException("AckMode must be MANUAL, "
										+ "MANUAL_IMMEDIATE, or MANUAL_IMMEDIATE_SYNC for manual acks");
							}
						}

						private void ackImmediate(final ConsumerRecord<K, V> record) {
							if (Thread.currentThread().equals(ListenerConsumer.this.consumerThread)) {
								Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
										new TopicPartition(record.topic(), record.partition()),
										new OffsetAndMetadata(record.offset() + 1));
								if (ListenerConsumer.this.logger.isDebugEnabled()) {
									ListenerConsumer.this.logger.debug("Committing: " + commits);
								}
								if (ListenerConsumer.this.ackMode.equals(AckMode.MANUAL_IMMEDIATE)) {
									ListenerConsumer.this.consumer.commitAsync(commits,
											ListenerConsumer.this.commitCallback);
								}
								else {
									ListenerConsumer.this.consumer.commitSync(commits);
								}
							}
							else {
								throw new IllegalStateException(
										"With " + ListenerConsumer.this.ackMode.name()
										+ " ack mode, acknowledge() must be invoked on the consumer thread");
							}
						}

						@Override
						public String toString() {
							return "Acknowledgment for " + record;
						}

					});
				}
				else {
					this.listener.onMessage(record);
				}
			}
			catch (Exception e) {
				if (getErrorHandler() != null) {
					getErrorHandler().handle(e, record);
				}
				else {
					this.logger.error("Listener threw an exception and no error handler for " + record, e);
				}
			}
		}

		private void processCommits(final AckMode ackMode, ConsumerRecords<K, V> records) {
			this.count += records.count();
			long now;
			if (ackMode.equals(AckMode.BATCH)) {
				if (!records.isEmpty()) {
					if (this.syncCommits) {
						this.consumer.commitSync();
					}
					else {
						this.consumer.commitAsync(this.commitCallback);
					}
				}
			}
			else if (!ackMode.equals(AckMode.MANUAL_IMMEDIATE) && !ackMode.equals(AckMode.MANUAL_IMMEDIATE_SYNC)) {
				if (!ackMode.equals(AckMode.MANUAL)) {
					updatePendingOffsets(records);
				}
				boolean countExceeded = this.count >= getAckCount();
				if (ackMode.equals(AckMode.COUNT) && countExceeded) {
					commitIfNecessary();
					this.count = 0;
				}
				else {
					now = System.currentTimeMillis();
					boolean elapsed = now - this.last > getAckTime();
					if (ackMode.equals(AckMode.TIME) && elapsed) {
						commitIfNecessary();
						this.last = now;
					}
					else if ((ackMode.equals(AckMode.COUNT_TIME) || ackMode.equals(AckMode.MANUAL))
							&& (elapsed || countExceeded)) {
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
			if (this.recentOffset > 0) {
				this.consumer.seekToEnd(
						this.definedPartitions.toArray(new TopicPartition[this.definedPartitions.size()]));
				for (TopicPartition topicPartition : this.definedPartitions) {
					long newOffset = this.consumer.position(topicPartition) - this.recentOffset;
					this.consumer.seek(topicPartition, newOffset);
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Reset " + topicPartition + " to offset " + newOffset);
					}
				}
			}
		}

		private void updatePendingOffsets(ConsumerRecords<K, V> records) {
			for (ConsumerRecord<K, V> record : records) {
				if (!this.offsets.containsKey(record.topic())) {
					this.offsets.put(record.topic(), new HashMap<Integer, Long>());
				}
				this.offsets.get(record.topic()).put(record.partition(), record.offset());
			}
		}

		private void updateManualOffset(ConsumerRecord<K, V> record) {
			if (!this.manualOffsets.containsKey(record.topic())) {
				this.manualOffsets.putIfAbsent(record.topic(), new ConcurrentHashMap<Integer, Long>());
			}
			this.manualOffsets.get(record.topic()).put(record.partition(), record.offset());
		}

		private void commitIfNecessary() {
			Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
			if (AckMode.MANUAL.equals(getAckMode())) {
				for (Entry<String, ConcurrentMap<Integer, Long>> entry : this.manualOffsets.entrySet()) {
					Iterator<Entry<Integer, Long>> iterator = entry.getValue().entrySet().iterator();
					while (iterator.hasNext()) {
						Entry<Integer, Long> offset = iterator.next();
						commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
								new OffsetAndMetadata(offset.getValue() + 1));
						iterator.remove();
					}
				}
			}
			else {
				for (Entry<String, Map<Integer, Long>> entry : this.offsets.entrySet()) {
					for (Entry<Integer, Long> offset : entry.getValue().entrySet()) {
						commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
								new OffsetAndMetadata(offset.getValue() + 1));
					}
				}
			}
			this.offsets.clear();
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Committing: " + commits);
			}
			if (!commits.isEmpty()) {
				if (this.syncCommits) {
					this.consumer.commitSync(commits);
				}
				else {
					this.consumer.commitAsync(commits, this.commitCallback);
				}
			}
		}
	}

	private static final class LoggingCommitCallback implements OffsetCommitCallback {

		private static final Log logger = LogFactory.getLog(LoggingCommitCallback.class);

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
