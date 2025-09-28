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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.jspecify.annotations.Nullable;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.util.Assert;

/**
 * Single-threaded share consumer container using the Java {@link ShareConsumer}.
 * <p>
 * This container provides support for Kafka share groups, enabling cooperative
 * consumption where multiple consumers can process records from the same partitions.
 * Unlike traditional consumer groups with exclusive partition assignment, share groups
 * allow load balancing at the record level.
 * <p>
 * Key features:
 * <ul>
 * <li>Explicit and implicit acknowledgment modes</li>
 * <li>Automatic error handling with REJECT acknowledgments</li>
 * <li>Poll-level acknowledgment constraints in explicit mode</li>
 * <li>Integration with Spring's {@code @KafkaListener} annotation</li>
 * </ul>
 * <p>
 * <strong>Acknowledgment Modes:</strong>
 * <ul>
 * <li><strong>Implicit</strong>: Records are automatically acknowledged as ACCEPT
 * after successful processing or REJECT on errors</li>
 * <li><strong>Explicit</strong>: Application must manually acknowledge each record;
 * subsequent polls are blocked until all records from the previous poll are acknowledged</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 * @since 4.0
 * @see ShareConsumer
 * @see ShareAcknowledgment
 */
public class ShareKafkaMessageListenerContainer<K, V>
		extends AbstractShareKafkaMessageListenerContainer<K, V> {

	private static final int POLL_TIMEOUT = 1000;

	@Nullable
	private String clientId;

	@SuppressWarnings("NullAway.Init")
	private volatile ShareListenerConsumer listenerConsumer;

	@SuppressWarnings("NullAway.Init")
	private volatile CompletableFuture<Void> listenerConsumerFuture;

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

	@Override
	public boolean isInExpectedState() {
		return isRunning();
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		ShareListenerConsumer listenerConsumerForMetrics = this.listenerConsumer;
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
		ContainerProperties containerProperties = getContainerProperties();
		Object messageListener = containerProperties.getMessageListener();
		AsyncTaskExecutor consumerExecutor = containerProperties.getListenerTaskExecutor();
		if (consumerExecutor == null) {
			consumerExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "" : getBeanName()) + "-C-");
			containerProperties.setListenerTaskExecutor(consumerExecutor);
		}
		GenericMessageListener<?> listener = (GenericMessageListener<?>) messageListener;
		Assert.state(listener != null, "'messageListener' cannot be null");

		// Validate listener type for explicit acknowledgment mode
		if (containerProperties.isExplicitShareAcknowledgment()) {
			boolean isAcknowledgingListener = listener instanceof AcknowledgingShareConsumerAwareMessageListener;
			Assert.state(isAcknowledgingListener,
					"Explicit acknowledgment mode requires an AcknowledgingShareConsumerAwareMessageListener. " +
					"Current listener type: " + listener.getClass().getName() + ". " +
					"Either use implicit acknowledgment mode or provide a listener that can handle acknowledgments.");
		}

		this.listenerConsumer = new ShareListenerConsumer(listener);
		setRunning(true);
		this.listenerConsumerFuture = CompletableFuture.runAsync(this.listenerConsumer, consumerExecutor);
	}

	@Override
	protected void doStop() {
		setRunning(false);
		// The consumer will exit its loop naturally when running becomes false.
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
	private static class PendingAcknowledgment<K, V> {

		private final ConsumerRecord<K, V> record;

		private final AcknowledgeType type;

		PendingAcknowledgment(ConsumerRecord<K, V> record, AcknowledgeType type) {
			this.record = record;
			this.type = type;
		}
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

		// Acknowledgment tracking for explicit mode
		private final Map<ConsumerRecord<K, V>, ShareConsumerAcknowledgment> pendingAcknowledgments = new ConcurrentHashMap<>();

		// Tracking for missed acknowledgment detection
		private final Map<ConsumerRecord<K, V>, Long> acknowledgmentTimestamps = new ConcurrentHashMap<>();

		// Lock for coordinating acknowledgment completion
		private final Object acknowledgmentLock = new Object();

		// Queue for acknowledgments from other threads
		private final ConcurrentLinkedQueue<PendingAcknowledgment<K, V>> acknowledgmentQueue = new ConcurrentLinkedQueue<>();

		private final boolean isExplicitMode;

		private final long ackTimeoutMs;

		ShareListenerConsumer(GenericMessageListener<?> listener) {
			this.consumer = ShareKafkaMessageListenerContainer.this.shareConsumerFactory.createShareConsumer(
					ShareKafkaMessageListenerContainer.this.getGroupId(),
					ShareKafkaMessageListenerContainer.this.getClientId());

			this.genericListener = listener;
			this.clientId = ShareKafkaMessageListenerContainer.this.getClientId();
			ContainerProperties containerProperties = getContainerProperties();

			// Configure acknowledgment mode
			this.isExplicitMode = containerProperties.isExplicitShareAcknowledgment();
			this.ackTimeoutMs = containerProperties.getShareAcknowledgmentTimeout().toMillis();

			// Configure consumer properties based on acknowledgment mode
			if (this.isExplicitMode) {
				// Apply explicit mode configuration to consumer
				// Note: This should ideally be done during consumer creation in the factory
				this.logger.info(() -> "Share consumer configured for explicit acknowledgment mode");
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
					// Process any pending acknowledgments from other threads
					processQueuedAcknowledgments();

					// In explicit mode, check for acknowledgment timeouts before polling
					if (this.isExplicitMode && !this.pendingAcknowledgments.isEmpty()) {
						checkAcknowledgmentTimeouts();
					}

					ConsumerRecords<K, V> records;
					try {
						records = this.consumer.poll(java.time.Duration.ofMillis(POLL_TIMEOUT));
					}
					catch (IllegalStateException e) {
						// KIP-932: In explicit mode, poll() throws if unacknowledged records exist
						if (this.isExplicitMode && !this.pendingAcknowledgments.isEmpty()) {
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
					if (this.isExplicitMode) {
						// Create acknowledgment using inner class
						acknowledgment = new ShareConsumerAcknowledgment(record);
						this.pendingAcknowledgments.put(record, acknowledgment);
						// Track when the record was dispatched for acknowledgment timeout detection
						this.acknowledgmentTimestamps.put(record, System.currentTimeMillis());
					}

					// Dispatch to listener
					if (this.genericListener instanceof AcknowledgingShareConsumerAwareMessageListener<?, ?> ackListener) {
						@SuppressWarnings("unchecked")
						AcknowledgingShareConsumerAwareMessageListener<K, V> typedAckListener =
								(AcknowledgingShareConsumerAwareMessageListener<K, V>) ackListener;
						typedAckListener.onShareRecord(record, acknowledgment, this.consumer);
					}
					else {
						// Basic listener remains the same
						@SuppressWarnings("unchecked")
						GenericMessageListener<ConsumerRecord<K, V>> listener =
								(GenericMessageListener<ConsumerRecord<K, V>>) this.genericListener;
						listener.onMessage(record);
					}
				}
				catch (Exception e) {
					handleProcessingError(record, acknowledgment, e);
				}
			}
			// Commit acknowledgments
			commitAcknowledgments();
		}

		private void handleProcessingError(ConsumerRecord<K, V> record,
				@Nullable ShareConsumerAcknowledgment acknowledgment, Exception e) {
			this.logger.error(e, "Error processing record: " + record);

			if (this.isExplicitMode && acknowledgment != null) {
				// Remove from pending and auto-reject on error
				this.pendingAcknowledgments.remove(record);
				try {
					acknowledgment.reject();
				}
				catch (Exception ackEx) {
					this.logger.error(ackEx, "Failed to reject record after processing error");
				}
			}
			else {
				// In implicit mode, auto-reject on error
				try {
					this.consumer.acknowledge(record, AcknowledgeType.REJECT);
				}
				catch (Exception ackEx) {
					this.logger.error(ackEx, "Failed to reject record after processing error");
				}
			}
		}

		private void commitAcknowledgments() {
			try {
				this.consumer.commitSync();
			}
			catch (Exception e) {
				this.logger.error(e, "Failed to commit acknowledgments");
			}
		}

		/**
		 * Called by ShareConsumerAcknowledgment when a record is acknowledged in explicit mode.
		 *
		 * @param record the record that was acknowledged
		 */
		void onRecordAcknowledged(ConsumerRecord<K, V> record) {
			if (this.isExplicitMode) {
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
				try {
					this.consumer.acknowledge(pendingAck.record, pendingAck.type);
					// Find and notify the acknowledgment object
					ShareConsumerAcknowledgment ack = this.pendingAcknowledgments.get(pendingAck.record);
					if (ack != null) {
						ack.notifyAcknowledged(pendingAck.type);
						onRecordAcknowledged(pendingAck.record);
					}
				}
				catch (Exception e) {
					this.logger.error(e, "Failed to process queued acknowledgment for record: " + pendingAck.record);
				}
			}
		}

		/**
		 * Check if any records have exceeded the acknowledgment timeout and log a warning.
		 */
		private void checkAcknowledgmentTimeouts() {
			if (!this.isExplicitMode || this.acknowledgmentTimestamps.isEmpty()) {
				return;
			}

			long currentTime = System.currentTimeMillis();
			for (Map.Entry<ConsumerRecord<K, V>, Long> entry : this.acknowledgmentTimestamps.entrySet()) {
				long recordAge = currentTime - entry.getValue();
				if (recordAge > this.ackTimeoutMs) {
					ConsumerRecord<K, V> record = entry.getKey();
					this.logger.warn(() -> String.format(
						"Record not acknowledged within timeout (%d seconds). " +
						"In explicit acknowledgment mode, you must call ack.acknowledge(), ack.release(), " +
						"or ack.reject() for every record. " +
						"Unacknowledged record: topic='%s', partition=%d, offset=%d",
						this.ackTimeoutMs / 1000,
						record.topic(), record.partition(), record.offset()
					));
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

		/**
		 * Inner acknowledgment class that integrates directly with the container.
		 */
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

			private void acknowledgeInternal(AcknowledgeType type) {
				if (!this.acknowledgmentType.compareAndSet(null, type)) {
					throw new IllegalStateException(
							String.format("Record at offset %d has already been acknowledged with type %s",
									this.record.offset(), this.acknowledgmentType.get()));
				}

				// Queue the acknowledgment to be processed on the consumer thread
				ShareKafkaMessageListenerContainer.this.listenerConsumer.acknowledgmentQueue.offer(
						new PendingAcknowledgment<>(this.record, type));
			}

			/**
			 * Called by the consumer thread after successful acknowledgment.
			 * @param type the type of acknowledgment
			 */
			void notifyAcknowledged(AcknowledgeType type) {
				this.acknowledgmentType.set(type);
			}

			boolean isAcknowledged() {
				return this.acknowledgmentType.get() != null;
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
