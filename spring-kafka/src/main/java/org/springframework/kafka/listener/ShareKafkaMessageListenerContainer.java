/*
 * Copyright 2025-present the original author or authors.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.jspecify.annotations.Nullable;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.log.LogMessage;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.util.Assert;

/**
 * Share consumer container using the Java {@link ShareConsumer}.
 * <p>
 * This container provides support for Kafka share groups, enabling cooperative
 * consumption where multiple consumers can process records from the same partitions.
 * Unlike traditional consumer groups with exclusive partition assignment, share groups
 * allow load balancing at the record level.
 * <p>
 * <strong>Concurrency Support:</strong>
 * <p>
 * This container supports running multiple consumer threads via the {@link #setConcurrency(int)}
 * method. Each thread creates its own {@link ShareConsumer} instance and polls independently.
 * Unlike traditional consumer groups where concurrency involves partition distribution,
 * share consumers leverage Kafka's record-level distribution across all group members.
 * This means multiple threads in the same container participate in the same share group,
 * with the broker distributing records across all consumer instances.
 * <p>
 * Key features:
 * <ul>
 * <li>Three acknowledgment modes: {@link ContainerProperties.ShareAckMode#EXPLICIT},
 * {@link ContainerProperties.ShareAckMode#MANUAL}, and {@link ContainerProperties.ShareAckMode#IMPLICIT}</li>
 * <li>Automatic error handling via {@link ShareConsumerRecordRecoverer} (default: REJECT)</li>
 * <li>Poll-level acknowledgment constraints in {@link ContainerProperties.ShareAckMode#MANUAL} mode</li>
 * <li>Integration with Spring's {@code @KafkaListener} annotation</li>
 * <li>Configurable concurrency for increased throughput</li>
 * </ul>
 * <p>
 * <strong>Acknowledgment Modes:</strong>
 * <ul>
 * <li><strong>EXPLICIT</strong> (default): Container-managed. The container sends ACCEPT after
 * successful processing and delegates error handling to the {@link ShareConsumerRecordRecoverer}
 * (default: REJECT). Equivalent to disabling {@code auto.commit} on a regular consumer.</li>
 * <li><strong>MANUAL</strong>: Listener-managed. The listener must acknowledge each record via
 * the provided {@link ShareAcknowledgment}. Subsequent polls are blocked until all records
 * from the previous poll are acknowledged.</li>
 * <li><strong>IMPLICIT</strong>: Kafka client implicit mode. The broker auto-accepts all records
 * regardless of processing outcome. No per-record acknowledgment control is available.</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 *
 * @since 4.0
 *
 * @see ShareConsumer
 * @see ShareAcknowledgment
 */
public class ShareKafkaMessageListenerContainer<K, V>
		extends AbstractShareKafkaMessageListenerContainer<K, V> {

	private static final int POLL_TIMEOUT = 1000;

	@Nullable
	private String clientId;

	private int concurrency = 1;

	private final List<ShareListenerConsumer> consumers = new ArrayList<>();

	private final List<CompletableFuture<Void>> consumerFutures = new ArrayList<>();

	private volatile CountDownLatch startLatch = new CountDownLatch(1);

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param shareConsumerFactory the share consumer factory
	 * @param containerProperties the container properties
	 */
	public ShareKafkaMessageListenerContainer(ShareConsumerFactory<? super K, ? super V> shareConsumerFactory,
			ContainerProperties containerProperties) {
		super(shareConsumerFactory, containerProperties);
		Assert.notNull(shareConsumerFactory, "A ShareConsumerFactory must be provided");
	}

	/**
	 * Get the {@code client.id} for the consumer.
	 * @return the client id, or null if not set
	 */
	@Nullable
	public String getClientId() {
		return this.clientId;
	}

	/**
	 * Set the {@code client.id} to use for the consumer.
	 * @param clientId the client id to set
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * Set the level of concurrency. This will create the specified number of
	 * consumer threads, each with its own {@link ShareConsumer} instance.
	 * All consumers participate in the same share group, leveraging Kafka's
	 * record-level distribution for load balancing.
	 * <p>
	 * Must be called before the container is started.
	 * @param concurrency the concurrency level (must be greater than 0)
	 */
	public void setConcurrency(int concurrency) {
		Assert.isTrue(concurrency > 0, "concurrency must be greater than 0");
		Assert.state(!isRunning(), "Cannot change concurrency while container is running");
		this.concurrency = concurrency;
	}

	@Override
	public boolean isInExpectedState() {
		return isRunning();
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		this.lifecycleLock.lock();
		try {
			if (this.consumers.isEmpty()) {
				return Collections.emptyMap();
			}
			Map<String, Map<MetricName, ? extends Metric>> allMetrics = new HashMap<>();
			for (ShareListenerConsumer consumer : this.consumers) {
				Map<MetricName, ? extends Metric> consumerMetrics = consumer.consumer.metrics();
				String consumerId = consumer.getClientId();
				if (consumerId != null) {
					allMetrics.put(consumerId, consumerMetrics);
				}
			}
			return Collections.unmodifiableMap(allMetrics);
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	protected void doStart() {
		if (isRunning()) {
			return;
		}
		ContainerProperties containerProperties = getContainerProperties();
		Object messageListener = containerProperties.getMessageListener();
		AsyncTaskExecutor consumerExecutor = containerProperties.getListenerTaskExecutor();
		if (consumerExecutor == null) {
			consumerExecutor = new SimpleAsyncTaskExecutor(getBeanName() + "-C-");
			containerProperties.setListenerTaskExecutor(consumerExecutor);
		}
		GenericMessageListener<?> listener = (GenericMessageListener<?>) messageListener;
		Assert.state(listener != null, "'messageListener' cannot be null");

		if (ContainerProperties.ShareAckMode.MANUAL.equals(containerProperties.getShareAckMode())) {
			boolean isAcknowledgingListener = listener instanceof AcknowledgingShareConsumerAwareMessageListener;
			Assert.state(isAcknowledgingListener,
					"ShareAckMode.MANUAL requires an AcknowledgingShareConsumerAwareMessageListener. " +
					"Current listener type: " + listener.getClass().getName() + ". " +
					"Either use ShareAckMode.EXPLICIT or provide a listener that accepts a ShareAcknowledgment parameter.");
		}

		if (ContainerProperties.ShareAckMode.EXPLICIT.equals(containerProperties.getShareAckMode())
				&& listener instanceof AcknowledgingShareConsumerAwareMessageListener) {
			this.logger.warn("Listener is an AcknowledgingShareConsumerAwareMessageListener but "
					+ "ShareAckMode.EXPLICIT is active; the acknowledgment argument will be null. "
					+ "Switch to ShareAckMode.MANUAL if the listener needs to manage acknowledgments.");
		}

		setRunning(true);

		for (int i = 0; i < this.concurrency; i++) {
			String consumerClientId = determineClientId(i);
			ShareListenerConsumer consumer = new ShareListenerConsumer(listener, consumerClientId);
			this.consumers.add(consumer);
			CompletableFuture<Void> future = CompletableFuture.runAsync(consumer, consumerExecutor);
			this.consumerFutures.add(future);
		}
	}

	private String determineClientId(int index) {
		String baseClientId = this.clientId != null ? this.clientId : getBeanName();
		if (this.concurrency > 1) {
			return baseClientId + "-" + index;
		}
		return baseClientId;
	}

	@Override
	protected void doStop() {
		setRunning(false);
		this.lifecycleLock.lock();
		try {
			CompletableFuture.allOf(this.consumerFutures.toArray(new CompletableFuture<?>[0])).join();
			this.consumers.clear();
			this.consumerFutures.clear();
		}
		catch (Exception e) {
			this.logger.error(e, "Error waiting for consumer threads to stop");
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	private void publishConsumerStartingEvent() {
		this.startLatch.countDown();
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerStartingEvent(this, this));
		}
	}

	private void publishConsumerStartedEvent() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerStartedEvent(this, this));
		}
	}

	/**
	 * Represents a pending acknowledgment to be processed on the consumer thread.
	 */
	private record PendingAcknowledgment<K, V>(ConsumerRecord<K, V> record, AcknowledgeType type) {
	}

	/**
	 * The inner share consumer thread that polls for records and dispatches to the listener.
	 */
	private class ShareListenerConsumer implements Runnable {

		private final LogAccessor logger = ShareKafkaMessageListenerContainer.this.logger;

		private final ShareConsumer<K, V> consumer;

		private final GenericMessageListener<?> genericListener;

		private final @Nullable String consumerGroupId = ShareKafkaMessageListenerContainer.this.getGroupId();

		private final @Nullable String clientId;

		private final Map<ConsumerRecord<K, V>, ShareConsumerAcknowledgment> pendingAcknowledgments = new ConcurrentHashMap<>();

		// Tracks per-record dispatch time for acknowledgment timeout detection
		private final Map<ConsumerRecord<K, V>, Long> acknowledgmentTimestamps = new ConcurrentHashMap<>();

		// Queue for acknowledgments submitted from listener threads to be applied on the consumer thread
		private final ConcurrentLinkedQueue<PendingAcknowledgment<K, V>> acknowledgmentQueue = new ConcurrentLinkedQueue<>();

		private final boolean isManualAckMode;

		private final long ackTimeoutMs;

		ShareListenerConsumer(GenericMessageListener<?> listener, String consumerClientId) {
			this.genericListener = listener;
			this.clientId = consumerClientId;
			ContainerProperties containerProperties = getContainerProperties();
			String groupId = ShareKafkaMessageListenerContainer.this.getGroupId();

			ContainerProperties.ShareAckMode shareAckMode = containerProperties.getShareAckMode();
			this.isManualAckMode = ContainerProperties.ShareAckMode.MANUAL.equals(shareAckMode);
			this.ackTimeoutMs = containerProperties.getShareAcknowledgmentTimeout().toMillis();

			if (ContainerProperties.ShareAckMode.IMPLICIT.equals(shareAckMode)) {
				ShareConsumerRecordRecoverer recoverer =
						ShareKafkaMessageListenerContainer.this.getShareConsumerRecordRecoverer();
				if (ShareConsumerRecordRecoverer.REJECTING != recoverer) {
					this.logger.warn("A custom ShareConsumerRecordRecoverer is configured but its acknowledgment "
							+ "decision will be ignored in ShareAckMode.IMPLICIT — all records are always "
							+ "ACCEPTed by the broker regardless of processing outcome.");
				}
				this.consumer = ShareKafkaMessageListenerContainer.this.shareConsumerFactory.createShareConsumer(
						groupId, consumerClientId);
			}
			else {
				// EXPLICIT and MANUAL both use Kafka client explicit mode so the container
				// has full per-record acknowledgment control.
				Object configured = ShareKafkaMessageListenerContainer.this.shareConsumerFactory
						.getConfigurationProperties()
						.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG);
				if (configured != null && ShareAcknowledgementMode.IMPLICIT.name().equalsIgnoreCase(configured.toString())) {
					this.logger.warn("Factory configuration has share.acknowledgement.mode=implicit "
							+ "but ShareAckMode." + shareAckMode + " is active; the container will "
							+ "override it with explicit mode. To use implicit mode, set "
							+ "ShareAckMode.IMPLICIT on the container properties instead.");
				}
				this.consumer = ShareKafkaMessageListenerContainer.this.shareConsumerFactory.createShareConsumer(
						groupId, consumerClientId,
						Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, ShareAcknowledgementMode.EXPLICIT.name()));
			}

			this.consumer.subscribe(Arrays.asList(containerProperties.getTopics()));
		}

		@Nullable
		String getClientId() {
			return this.clientId;
		}

		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public void run() {
			initialize();
			Throwable exitThrowable = null;
			while (isRunning()) {
				try {
					processQueuedAcknowledgments();

					// In MANUAL mode, check for acknowledgment timeouts before polling
					if (this.isManualAckMode && !this.pendingAcknowledgments.isEmpty()) {
						checkAcknowledgmentTimeouts();
					}

					ConsumerRecords<K, V> records;
					try {
						records = this.consumer.poll(java.time.Duration.ofMillis(POLL_TIMEOUT));
					}
					catch (RecordDeserializationException e) {
						// poll() throws when a record can't be deserialized. In EXPLICIT/MANUAL mode,
						// override the client's auto-release with REJECT to archive the record.
						// In IMPLICIT mode consumer.acknowledge() cannot be called — the broker
						// auto-accepts the record regardless of outcome.
						TopicPartition tp = e.topicPartition();
						long offset = e.offset();
						this.logger.warn(e, () -> "RecordDeserializationException at "
								+ tp + " offset " + offset + "; rejecting record and continuing");
						if (getContainerProperties().getShareAckMode() != ContainerProperties.ShareAckMode.IMPLICIT) {
							try {
								this.consumer.acknowledge(tp.topic(), tp.partition(), offset,
										AcknowledgeType.REJECT);
							}
							catch (Exception ackEx) {
								this.logger.error(ackEx, () -> "Failed to reject undeserializable record at "
										+ tp + " offset " + offset);
							}
						}
						continue;
					}
					catch (CorruptRecordException e) {
						// CRC check failure. The client automatically rejects the corrupt batch.
						this.logger.error(e, "CorruptRecordException during poll; "
								+ "Kafka client has auto-rejected the corrupt batch");
						continue;
					}
					catch (IllegalStateException e) {
						// In MANUAL mode, poll() throws if unacknowledged records exist
						if (this.isManualAckMode && !this.pendingAcknowledgments.isEmpty()) {
							this.logger.trace(() -> "Poll blocked waiting for " + this.pendingAcknowledgments.size() +
									" acknowledgments");
							// Small delay to prevent tight loop while maintaining reasonable responsiveness
							try {
								Thread.sleep(10);
							}
							catch (InterruptedException ie) {
								Thread.currentThread().interrupt();
								break;
							}
							continue;
						}
						throw e; // Re-throw if not related to acknowledgments
					}

					if (records != null && records.count() > 0) {
						processRecords(records);
					}
				}
				catch (Error e) {
					this.logger.error(e, "Stopping share consumer due to an Error");
					wrapUp();
					throw e;
				}
				catch (Exception e) {
					if (e.getCause() instanceof InterruptedException) {
						Thread.currentThread().interrupt();
					}
					this.logger.error(e, "Error in share consumer poll loop");
					exitThrowable = e;
					break;
				}
			}
			if (exitThrowable != null) {
				this.logger.error(exitThrowable, "ShareListenerConsumer exiting due to error");
			}
			wrapUp();
		}

		private void processRecords(ConsumerRecords<K, V> records) {
			for (var record : records) {
				ShareConsumerAcknowledgment acknowledgment = null;

				try {
					if (this.isManualAckMode) {
						acknowledgment = new ShareConsumerAcknowledgment(record);
						this.pendingAcknowledgments.put(record, acknowledgment);
						this.acknowledgmentTimestamps.put(record, System.currentTimeMillis());
					}

					if (this.genericListener instanceof AcknowledgingShareConsumerAwareMessageListener<?, ?> ackListener) {
						@SuppressWarnings("unchecked")
						AcknowledgingShareConsumerAwareMessageListener<K, V> typedAckListener =
								(AcknowledgingShareConsumerAwareMessageListener<K, V>) ackListener;
						typedAckListener.onShareRecord(record, acknowledgment, this.consumer);
					}
					else {
						@SuppressWarnings("unchecked")
						GenericMessageListener<ConsumerRecord<K, V>> listener =
								(GenericMessageListener<ConsumerRecord<K, V>>) this.genericListener;
						listener.onMessage(record);
					}

					// EXPLICIT: container sends ACCEPT on success.
					// IMPLICIT: broker auto-ACCEPTs — no acknowledge() call.
					// MANUAL: listener drives it via ShareAcknowledgment.
					if (getContainerProperties().getShareAckMode() == ContainerProperties.ShareAckMode.EXPLICIT) {
						this.consumer.acknowledge(record, AcknowledgeType.ACCEPT);
					}
				}
				catch (Exception e) {
					handleProcessingError(record, acknowledgment, e);
				}
			}
			commitAcknowledgments();
		}

		private void handleProcessingError(ConsumerRecord<K, V> record,
				@Nullable ShareConsumerAcknowledgment acknowledgment, Exception e) {

			AcknowledgeType action;
			try {
				action = ShareKafkaMessageListenerContainer.this.getShareConsumerRecordRecoverer().recover(record, e);
			}
			catch (Exception recovererEx) {
				this.logger.error(recovererEx, () -> "ShareConsumerRecordRecoverer threw an exception; "
						+ "falling back to REJECT for record from "
						+ record.topic() + "-" + record.partition() + "@" + record.offset());
				action = AcknowledgeType.REJECT;
			}

			// RENEW is not valid for error recovery (it extends lock during processing, not after failure)
			if (action == AcknowledgeType.RENEW) {
				this.logger.warn(() -> "ShareConsumerRecordRecoverer returned RENEW for record from "
						+ record.topic() + "-" + record.partition() + "@" + record.offset()
						+ "; RENEW is not valid for error recovery, using REJECT instead");
				action = AcknowledgeType.REJECT;
			}

			final AcknowledgeType actionToLog = action;

			if (this.isManualAckMode && acknowledgment != null) {
				// Remove from pending and from timestamp tracking so timeout checker doesn't hold stale entries
				this.pendingAcknowledgments.remove(record);
				this.acknowledgmentTimestamps.remove(record);
				try {
					switch (action) {
						case ACCEPT -> acknowledgment.acknowledge();
						case RELEASE -> acknowledgment.release();
						default -> acknowledgment.reject(); // REJECT, or any future AcknowledgeType
					}
				}
				catch (Exception ackEx) {
					this.logger.error(ackEx, () -> "Failed to " + actionToLog + " record after processing error");
				}
			}
			else if (getContainerProperties().getShareAckMode() != ContainerProperties.ShareAckMode.IMPLICIT) {
				try {
					this.consumer.acknowledge(record, action);
				}
				catch (Exception ackEx) {
					this.logger.error(ackEx, () -> "Failed to " + actionToLog + " record after processing error");
				}
			}
			// IMPLICIT mode: broker auto-ACCEPTs — no acknowledge() call is possible or needed
		}

		private void commitAcknowledgments() {
			try {
				this.consumer.commitSync();
			}
			catch (Exception e) {
				this.logger.error(e, "Failed to commit acknowledgments");
			}
		}

		void onRecordAcknowledged(ConsumerRecord<K, V> record) {
			if (this.isManualAckMode) {
				this.pendingAcknowledgments.remove(record);
				this.acknowledgmentTimestamps.remove(record);
				this.logger.debug(() -> "Record acknowledged, " + this.pendingAcknowledgments.size() + " still pending");
			}
		}

		/**
		 * Process acknowledgments queued from other threads.
		 * This ensures all consumer access happens on the consumer thread.
		 */
		private void processQueuedAcknowledgments() {
			PendingAcknowledgment<K, V> pendingAck;
			while ((pendingAck = this.acknowledgmentQueue.poll()) != null) {
				final PendingAcknowledgment<K, V> ack = pendingAck;
				try {
					this.consumer.acknowledge(ack.record(), ack.type());
					ShareConsumerAcknowledgment acknowledgment = this.pendingAcknowledgments.get(ack.record());
					if (acknowledgment != null) {
						acknowledgment.notifyAcknowledged(ack.type());
						// Remove from pending/timestamp tracking only on terminal ack (RENEW extends lock but record still in flight)
						if (ack.type() != AcknowledgeType.RENEW) {
							onRecordAcknowledged(ack.record());
						}
					}
				}
				catch (Exception e) {
					this.logger.error(e, () -> "Failed to process queued acknowledgment for record: " + ack.record());
				}
			}
		}

		private void checkAcknowledgmentTimeouts() {
			if (!this.isManualAckMode || this.acknowledgmentTimestamps.isEmpty()) {
				return;
			}

			long currentTime = System.currentTimeMillis();
			for (Map.Entry<ConsumerRecord<K, V>, Long> entry : this.acknowledgmentTimestamps.entrySet()) {
				long recordAge = currentTime - entry.getValue();
				if (recordAge > this.ackTimeoutMs) {
					ConsumerRecord<K, V> record = entry.getKey();
					this.logger.warn(LogMessage.format(
							"Record not acknowledged within timeout (%d seconds). "
							+ "In ShareAckMode.MANUAL you must call ack.acknowledge(), ack.release(), "
							+ "or ack.reject() for every record (call ack.renew() to extend the lock; a terminal ack is still required). "
							+ "Unacknowledged record: topic='%s', partition=%d, offset=%d",
							this.ackTimeoutMs / 1000,
							record.topic(), record.partition(), record.offset()));
					// Only warn once per record
					this.acknowledgmentTimestamps.put(record, currentTime);
				}
			}
		}

		protected void initialize() {
			publishConsumerStartingEvent();
			publishConsumerStartedEvent();
		}

		private void wrapUp() {
			this.consumer.close();
			this.logger.info(() -> this.consumerGroupId + ": Consumer stopped");
		}

		@Override
		public String toString() {
			return "ShareKafkaMessageListenerContainer.ShareListenerConsumer ["
					+ "consumerGroupId=" + this.consumerGroupId
					+ ", clientId=" + this.clientId
					+ "]";
		}

		private class ShareConsumerAcknowledgment implements ShareAcknowledgment {

			private final ConsumerRecord<K, V> record;

			private final AtomicReference<AcknowledgeType> acknowledgmentType = new AtomicReference<>();

			ShareConsumerAcknowledgment(ConsumerRecord<K, V> record) {
				this.record = record;
			}

			@Override
			public void acknowledge() {
				acknowledgeInternal(AcknowledgeType.ACCEPT);
			}

			@Override
			public void release() {
				acknowledgeInternal(AcknowledgeType.RELEASE);
			}

			@Override
			public void reject() {
				acknowledgeInternal(AcknowledgeType.REJECT);
			}

			@Override
			public void renew() {
				acknowledgeInternal(AcknowledgeType.RENEW);
			}

			@SuppressWarnings("NullAway") // Dataflow analysis limitation
			private void acknowledgeInternal(AcknowledgeType type) {
				if (type == AcknowledgeType.RENEW) {
					AcknowledgeType current = this.acknowledgmentType.get();
					if (current == AcknowledgeType.ACCEPT || current == AcknowledgeType.RELEASE || current == AcknowledgeType.REJECT) {
						throw new IllegalStateException(
								String.format("Record at offset %d has already been terminally acknowledged with type %s",
										this.record.offset(), current));
					}
					// Allow RENEW when state is null or already RENEW (multiple RENEWs permitted).
					// Try to transition null -> RENEW; if we fail, another thread changed the value.
					if (current != AcknowledgeType.RENEW && !this.acknowledgmentType.compareAndSet(null, AcknowledgeType.RENEW)) {
						// compareAndSet failed: state was updated by another thread. If it is now RENEW,
						// multiple RENEWs are allowed — queue this one. If it is a terminal type, throw below.
						if (this.acknowledgmentType.get() != AcknowledgeType.RENEW) {
							throw new IllegalStateException(
									String.format("Record at offset %d has already been acknowledged with type %s",
											this.record.offset(), this.acknowledgmentType.get()));
						}
					}
				}
				else {
					// Terminal: ACCEPT, RELEASE, or REJECT. Allowed only when state is null or RENEW.
					if (!this.acknowledgmentType.compareAndSet(null, type) && !this.acknowledgmentType.compareAndSet(AcknowledgeType.RENEW, type)) {
						throw new IllegalStateException(
								String.format("Record at offset %d has already been acknowledged with type %s",
										this.record.offset(), this.acknowledgmentType.get()));
					}
				}

				// Queue the acknowledgment to be processed on the consumer thread
				ShareListenerConsumer.this.acknowledgmentQueue.offer(
						new PendingAcknowledgment<>(this.record, type));
			}

		void notifyAcknowledged(AcknowledgeType type) {
				this.acknowledgmentType.set(type);
			}

			boolean isAcknowledged() {
				AcknowledgeType type = this.acknowledgmentType.get();
				return type != null && type != AcknowledgeType.RENEW;
			}

			@Nullable
			AcknowledgeType getAcknowledgmentType() {
				return this.acknowledgmentType.get();
			}

			ConsumerRecord<K, V> getRecord() {
				return this.record;
			}

			@Override
			public String toString() {
				return "ShareConsumerAcknowledgment{" +
						"topic=" + this.record.topic() +
						", partition=" + this.record.partition() +
						", offset=" + this.record.offset() +
						", acknowledged=" + isAcknowledged() +
						", type=" + getAcknowledgmentType() +
						'}';
			}
		}

	}

}
