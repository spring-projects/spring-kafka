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
import org.springframework.kafka.support.ShareAcknowledgmentException;
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
 * @author Soby Chacko
 * @since 4.0
 * @see ShareConsumer
 * @see ShareAcknowledgment
 * @see ContainerProperties.ShareAcknowledgmentMode
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

		private final boolean isExplicitMode;

		ShareListenerConsumer(GenericMessageListener<?> listener) {
			this.consumer = ShareKafkaMessageListenerContainer.this.shareConsumerFactory.createShareConsumer(
					ShareKafkaMessageListenerContainer.this.getGroupId(),
					ShareKafkaMessageListenerContainer.this.getClientId());

			this.genericListener = listener;
			this.clientId = ShareKafkaMessageListenerContainer.this.getClientId();
			ContainerProperties containerProperties = getContainerProperties();

			// Configure acknowledgment mode
			this.isExplicitMode = containerProperties.getShareAcknowledgmentMode() ==
					ContainerProperties.ShareAcknowledgmentMode.EXPLICIT;

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
					// Check acknowledgment constraints before polling
					if (this.isExplicitMode && !this.pendingAcknowledgments.isEmpty()) {
						// In explicit mode, all records from previous poll must be acknowledged
						this.logger.warn(() -> "Skipping poll - " + this.pendingAcknowledgments.size() +
								" records from previous poll still need acknowledgment");
						Thread.sleep(100); // Brief pause to avoid tight loop
						continue;
					}

					var records = this.consumer.poll(java.time.Duration.ofMillis(POLL_TIMEOUT));
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
					}

					// Dispatch to listener
					if (this.genericListener instanceof AcknowledgingShareConsumerAwareMessageListener<?, ?> ackListener) {
						@SuppressWarnings("unchecked")
						AcknowledgingShareConsumerAwareMessageListener<K, V> typedAckListener =
								(AcknowledgingShareConsumerAwareMessageListener<K, V>) ackListener;
						typedAckListener.onShareRecord(record, acknowledgment, this.consumer);  // Changed method name
					}
					else if (this.genericListener instanceof ShareConsumerAwareMessageListener<?, ?> consumerAwareListener) {
						@SuppressWarnings("unchecked")
						ShareConsumerAwareMessageListener<K, V> typedConsumerAwareListener =
								(ShareConsumerAwareMessageListener<K, V>) consumerAwareListener;
						typedConsumerAwareListener.onShareRecord(record, this.consumer);  // Changed method name
					}
					else {
						// Basic listener remains the same
						@SuppressWarnings("unchecked")
						GenericMessageListener<ConsumerRecord<K, V>> listener =
								(GenericMessageListener<ConsumerRecord<K, V>>) this.genericListener;
						listener.onMessage(record);
					}

					// Handle acknowledgment based on mode
					if (!this.isExplicitMode) {
						// In implicit mode, auto-acknowledge as ACCEPT
						this.consumer.acknowledge(record, AcknowledgeType.ACCEPT);
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
				this.logger.debug(() -> "Record acknowledged, " + this.pendingAcknowledgments.size() + " still pending");
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
			public void acknowledge(AcknowledgeType type) {
				Assert.notNull(type, "AcknowledgeType cannot be null");

				if (!this.acknowledgmentType.compareAndSet(null, type)) {
					throw new IllegalStateException(
							String.format("Record at offset %d has already been acknowledged with type %s",
									this.record.offset(), this.acknowledgmentType.get()));
				}

				try {
					// Direct access to container's consumer
					ShareKafkaMessageListenerContainer.this.listenerConsumer.consumer.acknowledge(this.record, type);

					// Direct notification to container
					ShareKafkaMessageListenerContainer.this.listenerConsumer.onRecordAcknowledged(this.record);

				}
				catch (Exception e) {
					// Reset state if acknowledgment failed
					this.acknowledgmentType.set(null);
					throw new ShareAcknowledgmentException(
							"Failed to acknowledge record at offset " + this.record.offset(), e);
				}
			}

			@Override
			public boolean isAcknowledged() {
				return this.acknowledgmentType.get() != null;
			}

			@Override
			@Nullable
			public AcknowledgeType getAcknowledgmentType() {
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
