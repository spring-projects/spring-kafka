/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.jspecify.annotations.Nullable;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.log.LogMessage;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent.Reason;
import org.springframework.kafka.event.ShareConsumerStoppingEvent;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
 * <li>Sync or async commit modes with optional {@link AcknowledgementCommitCallback} for async commit visibility</li>
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
 * @author Maxwell Balla
 * @author Youngjoo Kim
 * @author Omar Morales Ortega
 * @author OhKyu Chan
 * @author Jiyeon Kim
 * @author Ngoc Nhan
 * @author Minju Kim
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

	private volatile boolean consumerStoppedAbnormally;

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

	/**
	 * Return false if a consumer thread exited due to an exception or error while the
	 * container was running, or if the container was stopped with
	 * {@link #stopAbnormally(Runnable)}; the state is reset when the container is started
	 * again.
	 * @return true if the state is as expected.
	 */
	@Override
	public boolean isInExpectedState() {
		return !this.consumerStoppedAbnormally;
	}

	@Override
	public void stopAbnormally(Runnable callback) {
		this.lifecycleLock.lock();
		try {
			if (isRunning()) {
				this.consumerStoppedAbnormally = true;
			}
			stop();
		}
		finally {
			this.lifecycleLock.unlock();
		}
		callback.run();
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

	/**
	 * Get the broker-assigned client instance IDs, keyed by the client id of
	 * each running share consumer. The instance id correlates client metrics
	 * with broker-side telemetry and is distinct from the client id returned by
	 * {@link #getClientId()}. Returns an empty map when the container is not
	 * running.
	 * <p>
	 * The client instance id is requested from each consumer sequentially, so
	 * the worst-case wait is {@code concurrency x timeout}. Client telemetry
	 * must be enabled ({@code enable.metrics.push=true}, the default).
	 * @param timeout the maximum time to wait per consumer for the broker to
	 * assign an instance id (must be non-negative; zero means do not wait)
	 * @return the client instance ids keyed by client id; the value is
	 * {@code null} for a consumer whose id was not assigned within the timeout
	 * @since 4.2
	 * @see #metrics()
	 */
	public Map<String, Uuid> getClientInstanceIds(Duration timeout) {
		List<ShareListenerConsumer> snapshot;
		this.lifecycleLock.lock();
		try {
			snapshot = new ArrayList<>(this.consumers);
		}
		finally {
			this.lifecycleLock.unlock();
		}
		if (snapshot.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<String, Uuid> instanceIds = new LinkedHashMap<>();
		for (ShareListenerConsumer consumer : snapshot) {
			String consumerClientId = consumer.getClientId();
			if (consumerClientId != null) {
				instanceIds.put(consumerClientId, consumer.consumer.clientInstanceId(timeout));
			}
		}
		return Collections.unmodifiableMap(instanceIds);
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

		if (containerProperties.isShareAutoRenewLocks()
				&& !ContainerProperties.ShareAckMode.EXPLICIT.equals(containerProperties.getShareAckMode())) {
			this.logger.warn("shareAutoRenewLocks is enabled but ShareAckMode." + containerProperties.getShareAckMode()
					+ " is active; automatic lock renewal only applies to ShareAckMode.EXPLICIT and will be ignored.");
		}

		List<ShareListenerConsumer> builtConsumers = new ArrayList<>();
		try {
			for (int i = 0; i < this.concurrency; i++) {
				String consumerClientId = determineClientId(i);
				builtConsumers.add(new ShareListenerConsumer(listener, consumerClientId));
			}
		}
		catch (Exception e) {
			this.logger.error(e, "Failed to start share consumer");
			for (ShareListenerConsumer built : builtConsumers) {
				built.closeAfterFailedStartup();
			}
			publishConsumerFailedToStartEvent();
			throw e;
		}

		this.consumerStoppedAbnormally = false;
		setRunning(true);
		this.startLatch = new CountDownLatch(this.concurrency);

		for (ShareListenerConsumer consumer : builtConsumers) {
			this.consumers.add(consumer);
			CompletableFuture<Void> future = CompletableFuture.runAsync(consumer, consumerExecutor);
			this.consumerFutures.add(future);
		}
		try {
			if (!this.startLatch.await(containerProperties.getConsumerStartTimeout().toMillis(),
					TimeUnit.MILLISECONDS)) {

				this.logger.error("Consumer thread failed to start - does the configured task executor "
						+ "have enough threads to support all containers and concurrency?");
				publishConsumerFailedToStartEvent();
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private String determineClientId(int index) {
		String baseClientId = this.clientId;
		if (baseClientId == null) {
			String propertiesClientId = getContainerProperties().getClientId();
			baseClientId = StringUtils.hasText(propertiesClientId) ? propertiesClientId : getBeanName();
		}
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

	private void publishConsumerStoppedEvent(@Nullable Throwable throwable) {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			Reason reason;
			if (throwable instanceof Error) {
				reason = Reason.ERROR;
			}
			else if (throwable != null) {
				reason = Reason.ABNORMAL;
			}
			else {
				reason = Reason.NORMAL;
			}
			publisher.publishEvent(new ConsumerStoppedEvent(this, this, reason));
		}
	}

	private void publishConsumerStoppingEvent(ShareConsumer<?, ?> consumer) {
		try {
			ApplicationEventPublisher publisher = getApplicationEventPublisher();
			if (publisher != null) {
				publisher.publishEvent(new ShareConsumerStoppingEvent(this, this, consumer));
			}
		}
		catch (Exception e) {
			this.logger.error(e, "Failed to publish share consumer stopping event");
		}
	}

	private void publishConsumerFailedToStartEvent() {
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			publisher.publishEvent(new ConsumerFailedToStartEvent(this, this));
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

		private LogAccessor logger = ShareKafkaMessageListenerContainer.this.logger;

		private final ShareConsumer<K, V> consumer;

		private final GenericMessageListener<?> genericListener;

		private final @Nullable String consumerGroupId = ShareKafkaMessageListenerContainer.this.getGroupId();

		private final @Nullable String clientId;

		private final Map<ConsumerRecord<K, V>, ShareConsumerAcknowledgment> pendingAcknowledgments = new ConcurrentHashMap<>();

		// Tracks per-record dispatch time for acknowledgment timeout detection
		private final Map<ConsumerRecord<K, V>, Long> acknowledgmentTimestamps = new ConcurrentHashMap<>();

		// Queue for acknowledgments submitted from listener threads to be applied on the consumer thread
		private final ConcurrentLinkedQueue<PendingAcknowledgment<K, V>> acknowledgmentQueue = new ConcurrentLinkedQueue<>();

		// Acknowledgments the client rejected while a renewal of their record was in progress, retried
		// in order before newer acknowledgments. Only accessed on the consumer thread.
		private final List<PendingAcknowledgment<K, V>> deferredAcknowledgments = new ArrayList<>();

		private final boolean isManualAckMode;

		private final boolean syncShareCommits;

		private final long ackTimeoutMs;

		private final boolean autoRenewLocks;

		private final long lockRenewIntervalMs;

		private final @Nullable ExecutorService dispatchExecutor;

		// Records acquired with automatic lock renewal that have not been terminally acknowledged
		// yet, with their renewal state. Only accessed on the consumer thread.
		private final Map<ConsumerRecord<K, V>, RenewalState> outstandingRecords = new LinkedHashMap<>();

		private final Queue<ConsumerRecord<K, V>> recordsToDispatch = new ArrayDeque<>();

		private @Nullable ConsumerRecord<K, V> inFlightRecord;

		// True when the in-flight record is no longer acquired by this consumer (redelivered, or its
		// renewal was never confirmed), so its outcome must not be acknowledged.
		private boolean inFlightAbandoned;

		// True after commitSync() failed to send renewals: they may still be in progress in the client,
		// and a poll() would then acquire new records, so do not poll until a later commitSync() succeeds
		// or no record is being renewed anymore.
		private boolean renewalsUnconfirmed;

		// Limits the warning for a rejected poll() to one per occurrence
		private boolean pollRejectionLogged;

		private @Nullable Future<?> inFlightFuture;

		private final AtomicReference<@Nullable Throwable> inFlightError = new AtomicReference<>();

		ShareListenerConsumer(GenericMessageListener<?> listener, @Nullable String consumerClientId) {
			this.genericListener = listener;
			this.clientId = consumerClientId;
			ContainerProperties containerProperties = getContainerProperties();
			String groupId = ShareKafkaMessageListenerContainer.this.getGroupId();

			ContainerProperties.ShareAckMode shareAckMode = containerProperties.getShareAckMode();
			this.isManualAckMode = ContainerProperties.ShareAckMode.MANUAL.equals(shareAckMode);
			this.syncShareCommits = containerProperties.isSyncShareCommits();
			this.ackTimeoutMs = containerProperties.getShareAcknowledgmentTimeout().toMillis();
			this.autoRenewLocks = ContainerProperties.ShareAckMode.EXPLICIT.equals(shareAckMode)
					&& containerProperties.isShareAutoRenewLocks();
			this.lockRenewIntervalMs = Math.max(1, this.ackTimeoutMs / 2);
			this.dispatchExecutor = this.autoRenewLocks ? newDispatchExecutor(consumerClientId) : null;

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

			// Register acknowledgement commit callback
			AcknowledgementCommitCallback callback = containerProperties.getAcknowledgementCommitCallback();
			if (callback != null) {
				this.consumer.setAcknowledgementCommitCallback(callback);
			}
			else if (!this.syncShareCommits) {
				// Default callback for async mode: log failures
				this.consumer.setAcknowledgementCommitCallback((offsets, exception) -> {
					if (exception != null) {
						this.logger.error(exception, () -> "Async acknowledgement commit failed for offsets: " + offsets);
					}
				});
			}

			String[] topics = containerProperties.getTopics();
			Assert.notNull(topics,  "topics cannot be null");
			this.consumer.subscribe(Arrays.asList(topics));
		}

		private static ExecutorService newDispatchExecutor(@Nullable String consumerClientId) {
			return Executors.newSingleThreadExecutor(runnable -> {
				Thread thread = new Thread(runnable,
						(consumerClientId != null ? consumerClientId : "share") + "-dispatch");
				thread.setDaemon(true);
				return thread;
			});
		}

		/**
		 * Quietly close the underlying {@link ShareConsumer} after a failed startup.
		 * No lifecycle events are published because the consumer never reached {@code run()}.
		 */
		private void closeAfterFailedStartup() {
			try {
				this.consumer.close();
			}
			catch (Exception closeEx) {
				this.logger.error(closeEx, "Failed to close share consumer after startup failure");
			}
		}

		@Nullable
		String getClientId() {
			return this.clientId;
		}

		@Override
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

					if (this.autoRenewLocks) {
						processInFlightRecords();
						if (!isPollAllowed()) {
							// In explicit mode the client allows a poll only once every record it can
							// acknowledge has an acknowledgment; records with a RENEW sent are returned
							// by poll() again, so wait until all outstanding records are being renewed.
							try {
								Thread.sleep(10);
							}
							catch (InterruptedException ie) {
								Thread.currentThread().interrupt();
								break;
							}
							continue;
						}
					}

					ConsumerRecords<K, V> records;
					try {
						records = pollHandlingRecordErrors(java.time.Duration.ofMillis(POLL_TIMEOUT));
					}
					catch (IllegalStateException e) {
						// poll() throws if unacknowledged records exist: expected in MANUAL mode while the
						// listener has not acknowledged yet; with automatic lock renewal the container
						// tracks this itself, so it indicates a mismatch with the client.
						if (this.autoRenewLocks && !this.outstandingRecords.isEmpty() && !this.pollRejectionLogged) {
							this.pollRejectionLogged = true;
							this.logger.warn(e, "Poll rejected while records are outstanding; retrying");
						}
						if ((this.isManualAckMode && !this.pendingAcknowledgments.isEmpty())
								|| (this.autoRenewLocks && !this.outstandingRecords.isEmpty())) {
							this.logger.trace(() -> "Poll blocked waiting for acknowledgments");
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

					this.pollRejectionLogged = false;
					if (records != null && records.count() > 0) {
						processRecords(records);
					}
				}
				catch (Error e) {
					this.logger.error(e, "Stopping share consumer due to an Error");
					ShareKafkaMessageListenerContainer.this.consumerStoppedAbnormally = true;
					wrapUp(e);
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
				ShareKafkaMessageListenerContainer.this.consumerStoppedAbnormally = true;
			}
			wrapUp(exitThrowable);
		}

		/**
		 * Poll, handling the errors for records that cannot be returned: an undeserializable
		 * record is rejected (the client would otherwise release it), and a corrupt batch is
		 * rejected by the client. No records are returned in these cases.
		 * @param timeout the poll timeout.
		 * @return the polled records.
		 */
		private ConsumerRecords<K, V> pollHandlingRecordErrors(Duration timeout) {
			try {
				return this.consumer.poll(timeout);
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
			}
			catch (CorruptRecordException e) {
				// CRC check failure. The client automatically rejects the corrupt batch.
				this.logger.error(e, "CorruptRecordException during poll; "
						+ "Kafka client has auto-rejected the corrupt batch");
			}
			return ConsumerRecords.empty();
		}

		private void processRecords(ConsumerRecords<K, V> records) {
			if (this.autoRenewLocks) {
				// Dispatch one record at a time on the dispatch thread so the consumer thread
				// stays free to renew acquisition locks while processing is in flight;
				// see processInFlightRecords().
				trackPolledRecords(records);
				processInFlightRecords();
				return;
			}
			for (var record : records) {
				if (this.isManualAckMode && isRenewedRecordReturned(record)) {
					// After a RENEW is sent, poll() returns the record again; it is still being
					// processed by the listener, so it must not be dispatched a second time.
					continue;
				}
				ShareConsumerAcknowledgment acknowledgment = null;

				try {
					if (this.isManualAckMode) {
						acknowledgment = new ShareConsumerAcknowledgment(record);
						this.pendingAcknowledgments.put(record, acknowledgment);
						this.acknowledgmentTimestamps.put(record, System.currentTimeMillis());
					}

					invokeListener(record, acknowledgment);

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

		/**
		 * Determine whether a polled record is a record still pending acknowledgment that poll()
		 * returned after a renewal. A pending record returned with a higher delivery count was
		 * acquired again (for example, because its lock expired before it was renewed); the
		 * previous attempt is retired so that the record is dispatched as a new delivery.
		 * @param record the polled record.
		 * @return true if the record is a renewal return and must not be dispatched.
		 */
		private boolean isRenewedRecordReturned(ConsumerRecord<K, V> record) {
			for (ConsumerRecord<K, V> pending : this.pendingAcknowledgments.keySet()) {
				if (pending.offset() == record.offset() && pending.partition() == record.partition()
						&& pending.topic().equals(record.topic())) {
					if (isRedelivery(pending, record)) {
						retire(pending);
						return false;
					}
					return true;
				}
			}
			return false;
		}

		private static boolean isRedelivery(ConsumerRecord<?, ?> pending, ConsumerRecord<?, ?> polled) {
			Optional<Short> pendingCount = pending.deliveryCount();
			Optional<Short> polledCount = polled.deliveryCount();
			return pendingCount.isPresent() && polledCount.isPresent() && polledCount.get() > pendingCount.get();
		}

		private void retire(ConsumerRecord<K, V> record) {
			this.logger.warn(() -> "Record was redelivered while still pending acknowledgment, probably because its "
					+ "acquisition lock expired; acknowledgments for the previous delivery will be discarded: " + record);
			ShareConsumerAcknowledgment acknowledgment = this.pendingAcknowledgments.remove(record);
			this.acknowledgmentTimestamps.remove(record);
			if (acknowledgment != null) {
				// After this, the acknowledgment no longer queues anything, so removing what it
				// queued so far discards every acknowledgment for the previous delivery.
				acknowledgment.retire();
			}
			this.deferredAcknowledgments.removeIf(ack -> ack.record() == record);
			this.acknowledgmentQueue.removeIf(ack -> ack.record() == record);
		}

		private void invokeListener(ConsumerRecord<K, V> record,
				@Nullable ShareConsumerAcknowledgment acknowledgment) throws Exception {

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
		}

		private boolean hasOutstandingRecords() {
			return this.inFlightRecord != null || !this.outstandingRecords.isEmpty();
		}

		/**
		 * The client allows a poll in explicit mode only when every record it can currently
		 * acknowledge has an acknowledgment, so with automatic renewal a poll is allowed only
		 * when every outstanding record has a RENEW sent and is waiting to be returned.
		 * @return true if poll() can be called.
		 */
		private boolean isPollAllowed() {
			if (this.renewalsUnconfirmed) {
				if (this.outstandingRecords.values().stream().anyMatch(state -> state.renewing)) {
					return false;
				}
				this.renewalsUnconfirmed = false;
			}
			for (RenewalState state : this.outstandingRecords.values()) {
				if (!state.renewing) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Drive the records acquired with automatic lock renewal, on the consumer thread:
		 * complete the acknowledgment of the record whose listener invocation has finished,
		 * dispatch the next queued record, give up on renewals that were never confirmed,
		 * and renew acquisition locks when due.
		 */
		private void processInFlightRecords() {
			completeInFlightRecordIfDone();

			if (this.inFlightRecord == null) {
				ConsumerRecord<K, V> next = this.recordsToDispatch.poll();
				if (next != null) {
					dispatchRecord(next);
				}
			}

			expireUnconfirmedRenewals();
			renewLocksIfDue();
		}

		/**
		 * Track the records returned by poll(): a record already outstanding with the same
		 * delivery count is a renewal return and can be acknowledged again; one with a higher
		 * delivery count was acquired again after its lock was lost, so the previous delivery is
		 * abandoned and the record is dispatched as a new delivery; any other record is new.
		 * @param records the polled records.
		 */
		private void trackPolledRecords(ConsumerRecords<K, V> records) {
			long now = System.currentTimeMillis();
			for (var record : records) {
				ConsumerRecord<K, V> outstanding = findOutstanding(record);
				if (outstanding != null) {
					if (!isRedelivery(outstanding, record)) {
						RenewalState state = this.outstandingRecords.get(outstanding);
						if (state != null) {
							state.returned();
						}
						continue;
					}
					abandon(outstanding, "it was redelivered, probably because its acquisition lock expired");
				}
				this.outstandingRecords.put(record, new RenewalState(now));
				this.recordsToDispatch.add(record);
			}
		}

		private @Nullable ConsumerRecord<K, V> findOutstanding(ConsumerRecord<K, V> record) {
			for (ConsumerRecord<K, V> outstanding : this.outstandingRecords.keySet()) {
				if (outstanding.offset() == record.offset() && outstanding.partition() == record.partition()
						&& outstanding.topic().equals(record.topic())) {
					return outstanding;
				}
			}
			return null;
		}

		/**
		 * Stop tracking a record that is no longer acquired by this consumer: its outcome is not
		 * acknowledged when it completes, and it is not dispatched if it is still queued.
		 * @param record the record.
		 * @param reason the reason, for logging.
		 */
		private void abandon(ConsumerRecord<K, V> record, String reason) {
			this.outstandingRecords.remove(record);
			// The client returns a renewed record as the same instance, so a delivery of the record can be
			// queued while a previous, abandoned delivery of the same instance is still in flight
			if (this.recordsToDispatch.remove(record)) {
				this.logger.warn(() -> "No longer tracking record because " + reason
						+ "; it will not be dispatched: " + record);
			}
			if (record == this.inFlightRecord && !this.inFlightAbandoned) {
				this.inFlightAbandoned = true;
				this.logger.warn(() -> "No longer tracking record because " + reason
						+ "; its processing outcome will not be acknowledged: " + record);
			}
		}

		private void expireUnconfirmedRenewals() {
			long now = System.currentTimeMillis();
			long limit = Math.max(this.ackTimeoutMs, 3L * POLL_TIMEOUT);
			List<ConsumerRecord<K, V>> expired = new ArrayList<>();
			this.outstandingRecords.forEach((record, state) -> {
				if (state.renewing && now - state.renewSentAt > limit) {
					expired.add(record);
				}
			});
			expired.forEach(record -> abandon(record, "poll() did not return it within " + limit
					+ " ms after its renewal; its lock may have expired"));
		}

		/**
		 * Acknowledge the in-flight record if its listener invocation has finished and the
		 * record can be acknowledged; while a renewal of it is in progress, wait for poll() to
		 * return it first.
		 * @return true if there is no longer a record in flight
		 */
		private boolean completeInFlightRecordIfDone() {
			ConsumerRecord<K, V> record = this.inFlightRecord;
			Future<?> future = this.inFlightFuture;
			if (record == null) {
				return true;
			}
			if (future == null || !future.isDone()) {
				return false;
			}
			// An abandoned delivery must not look up the state, which may belong to a new delivery
			// of the same record instance
			RenewalState state = this.inFlightAbandoned ? null : this.outstandingRecords.get(record);
			if (state != null && state.renewing) {
				return false;
			}
			Throwable error = this.inFlightError.get();
			boolean abandoned = state == null;
			this.inFlightRecord = null;
			this.inFlightFuture = null;
			this.inFlightError.set(null);
			this.inFlightAbandoned = false;
			if (abandoned) {
				// Already removed when abandoned; the client may since have returned the same record
				// instance as a new delivery, which is now tracked under the same key
				if (error instanceof Error err) {
					throw err;
				}
				return true;
			}
			this.outstandingRecords.remove(record);
			if (error == null) {
				try {
					this.consumer.acknowledge(record, AcknowledgeType.ACCEPT);
				}
				catch (Exception ackEx) {
					this.logger.error(ackEx, () -> "Failed to ACCEPT record after processing: " + record);
				}
			}
			else if (error instanceof Exception exception) {
				handleProcessingError(record, null, exception);
			}
			else {
				throw (Error) error;
			}
			commitAcknowledgments();
			return true;
		}

		/**
		 * On stop, wait up to the shutdown timeout for the in-flight record to complete so
		 * that its outcome is acknowledged, renewing locks in the meantime, and release the
		 * records that were never dispatched so that they are redelivered right away.
		 */
		private void drainInFlightRecords() {
			if (!this.autoRenewLocks || !hasOutstandingRecords()) {
				return;
			}
			long deadline = System.currentTimeMillis() + getContainerProperties().getShutdownTimeout();
			try {
				while (!completeInFlightRecordIfDone()) {
					if (System.currentTimeMillis() >= deadline) {
						ConsumerRecord<K, V> record = this.inFlightRecord;
						Future<?> future = this.inFlightFuture;
						String reason = future != null && future.isDone()
								? "Renewed record was not returned within the shutdown timeout"
								: "Listener did not complete within the shutdown timeout";
						this.logger.warn(() -> reason + "; the record will be redelivered: " + record);
						break;
					}
					expireUnconfirmedRenewals();
					renewLocksIfDue();
					if (isPollAllowed() && !this.outstandingRecords.isEmpty()) {
						trackPolledRecords(pollHandlingRecordErrors(Duration.ofMillis(100)));
					}
					else {
						Thread.sleep(10);
					}
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			catch (Error error) {
				this.logger.error(error, "Listener threw an Error while the share consumer was stopping");
			}
			catch (Exception ex) {
				this.logger.error(ex, "Failed to complete the in-flight record while the share consumer was stopping");
			}
			ConsumerRecord<K, V> queued;
			while ((queued = this.recordsToDispatch.poll()) != null) {
				ConsumerRecord<K, V> record = queued;
				RenewalState state = this.outstandingRecords.remove(record);
				if (state == null || state.renewing) {
					// Cannot be acknowledged until returned by poll(); the lock is released on close
					continue;
				}
				try {
					this.consumer.acknowledge(record, AcknowledgeType.RELEASE);
				}
				catch (Exception ackEx) {
					this.logger.error(ackEx, () -> "Failed to release undispatched record: " + record);
				}
			}
			commitAcknowledgments();
		}

		private boolean isCompletedInFlightRecordRenewing() {
			ConsumerRecord<K, V> record = this.inFlightRecord;
			Future<?> future = this.inFlightFuture;
			if (record == null || future == null || !future.isDone() || this.inFlightAbandoned) {
				return false;
			}
			RenewalState state = this.outstandingRecords.get(record);
			return state != null && state.renewing && System.currentTimeMillis() - state.renewSentAt >= POLL_TIMEOUT;
		}

		private void dispatchRecord(ConsumerRecord<K, V> record) {
			ExecutorService executor = this.dispatchExecutor;
			Assert.state(executor != null, "dispatchExecutor must not be null when autoRenewLocks is enabled");
			this.inFlightRecord = record;
			this.inFlightFuture = executor.submit(() -> {
				try {
					invokeListener(record, null);
				}
				catch (Throwable ex) { // captured for completion handling on the consumer thread
					this.inFlightError.set(ex);
				}
			});
		}

		/**
		 * Renew the locks of all records that can be acknowledged once any of them is due, so
		 * that poll() is allowed and returns the renewed records. Renew them early when the
		 * in-flight record has completed but cannot be acknowledged until poll() returns it;
		 * renewing a lock early only extends it.
		 */
		private void renewLocksIfDue() {
			if (this.outstandingRecords.isEmpty()) {
				return;
			}
			long now = System.currentTimeMillis();
			boolean due = isCompletedInFlightRecordRenewing();
			for (RenewalState state : this.outstandingRecords.values()) {
				if (!state.renewing && now - state.renewedAt >= this.lockRenewIntervalMs) {
					due = true;
					break;
				}
			}
			if (!due) {
				return;
			}
			boolean renewed = false;
			for (Map.Entry<ConsumerRecord<K, V>, RenewalState> entry : this.outstandingRecords.entrySet()) {
				RenewalState state = entry.getValue();
				if (!state.renewing) {
					try {
						this.consumer.acknowledge(entry.getKey(), AcknowledgeType.RENEW);
						state.renewSent(now);
						renewed = true;
					}
					catch (Exception e) {
						this.logger.error(e, () -> "Failed to renew acquisition lock for record: " + entry.getKey());
					}
				}
			}
			if (renewed) {
				sendRenewalsAndReturnRecords();
			}
		}

		/**
		 * Send the renewals synchronously, so that the following poll() returns the renewed
		 * records without fetching new ones: while renewals are in progress, a poll() acquires
		 * new records that are not returned, and not renewed, until the renewals complete.
		 * Records whose renewal failed are no longer acquired and are abandoned.
		 */
		private void sendRenewalsAndReturnRecords() {
			Map<TopicIdPartition, Optional<KafkaException>> result;
			try {
				result = this.consumer.commitSync();
			}
			catch (Exception ex) {
				// The client drops renewals that time out, so the locks are treated as lost: the records
				// are abandoned after the acknowledgment timeout and processed again if redelivered
				this.renewalsUnconfirmed = true;
				this.logger.error(ex, "Failed to send acquisition lock renewals");
				return;
			}
			this.renewalsUnconfirmed = false;
			result.forEach((partition, error) -> error.ifPresent(ex -> abandonRenewing(partition, ex)));
			try {
				trackPolledRecords(pollHandlingRecordErrors(Duration.ZERO));
			}
			catch (Exception ex) {
				this.logger.error(ex, "Failed to poll for records after renewing their acquisition locks");
			}
		}

		private void abandonRenewing(TopicIdPartition partition, KafkaException ex) {
			List<ConsumerRecord<K, V>> failed = new ArrayList<>();
			this.outstandingRecords.forEach((record, state) -> {
				if (state.renewing && record.partition() == partition.partition()
						&& record.topic().equals(partition.topic())) {
					failed.add(record);
				}
			});
			failed.forEach(record -> abandon(record, "its lock renewal failed (" + ex.getMessage() + ")"));
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
				if (this.syncShareCommits) {
					this.consumer.commitSync();
				}
				else {
					this.consumer.commitAsync();
				}
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
			// Deferred acknowledgments go first so that, per record, acknowledgments are applied in the
			// order they were made; while one is deferred, later ones for the same record wait behind it.
			List<PendingAcknowledgment<K, V>> toProcess = new ArrayList<>(this.deferredAcknowledgments);
			this.deferredAcknowledgments.clear();
			PendingAcknowledgment<K, V> queued;
			while ((queued = this.acknowledgmentQueue.poll()) != null) {
				toProcess.add(queued);
			}
			Set<ConsumerRecord<K, V>> deferredRecords = Collections.newSetFromMap(new IdentityHashMap<>());
			for (PendingAcknowledgment<K, V> ack : toProcess) {
				if (deferredRecords.contains(ack.record())) {
					this.deferredAcknowledgments.add(ack);
					continue;
				}
				if (ack.type() == AcknowledgeType.RENEW && !this.pendingAcknowledgments.containsKey(ack.record())) {
					// The record was already acknowledged terminally; a late RENEW must not replace that
					// acknowledgment in the client, which keeps the last type set for a record.
					this.logger.debug(() -> "Discarding RENEW for a record that is no longer pending: " + ack.record());
					continue;
				}
				try {
					try {
						this.consumer.acknowledge(ack.record(), ack.type());
					}
					catch (IllegalStateException ex) {
						if (!this.pendingAcknowledgments.containsKey(ack.record())) {
							throw ex;
						}
						// Typically a RENEW for this record has been sent and poll() has not returned the
						// record yet, so the client does not accept another acknowledgment for it.
						this.logger.debug(() -> "Record could not be acknowledged with " + ack.type()
								+ "; retrying after poll: " + ack.record());
						this.deferredAcknowledgments.add(ack);
						deferredRecords.add(ack.record());
						continue;
					}
					ShareConsumerAcknowledgment acknowledgment = this.pendingAcknowledgments.get(ack.record());
					if (acknowledgment != null) {
						acknowledgment.notifyAcknowledged(ack.type());
						// Remove from pending/timestamp tracking only on terminal ack (RENEW extends lock but record still in flight)
						if (ack.type() != AcknowledgeType.RENEW) {
							onRecordAcknowledged(ack.record());
						}
						else {
							// The lock was extended, so restart the acknowledgment timeout for this record
							this.acknowledgmentTimestamps.computeIfPresent(ack.record(),
									(record, timestamp) -> System.currentTimeMillis());
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

		private void wrapUp(@Nullable Throwable throwable) {
			if (!(throwable instanceof Error)) {
				drainInFlightRecords();
			}
			if (this.dispatchExecutor != null) {
				this.dispatchExecutor.shutdown();
			}
			publishConsumerStoppingEvent(this.consumer);
			if (!this.acknowledgmentQueue.isEmpty() || !this.deferredAcknowledgments.isEmpty()) {
				// Acknowledgments queued after the last drain - the listener acknowledged during the
				// final batch, or from another thread - would otherwise be discarded with the consumer
				// and their records redelivered.
				processQueuedAcknowledgments();
				commitAcknowledgments();
			}
			if (!this.deferredAcknowledgments.isEmpty()) {
				this.logger.warn(() -> "Discarding " + this.deferredAcknowledgments.size() + " acknowledgment(s) "
						+ "that could not be applied before the consumer closed; the records will be redelivered");
			}
			this.consumer.close();
			this.logger.info(() -> this.consumerGroupId + ": Consumer stopped");
			publishConsumerStoppedEvent(throwable);
		}

		@Override
		public String toString() {
			return "ShareKafkaMessageListenerContainer.ShareListenerConsumer ["
					+ "consumerGroupId=" + this.consumerGroupId
					+ ", clientId=" + this.clientId
					+ "]";
		}

		/**
		 * Renewal state of a record acquired with automatic lock renewal.
		 */
		private static final class RenewalState {

			private boolean renewing;

			private long renewedAt;

			private long renewSentAt;

			RenewalState(long acquiredAt) {
				this.renewedAt = acquiredAt;
			}

			void renewSent(long now) {
				this.renewing = true;
				this.renewSentAt = now;
			}

			void returned() {
				// The lock was extended when the renewal was sent, not when poll() returned the record
				this.renewing = false;
				this.renewedAt = this.renewSentAt;
			}

		}

		private class ShareConsumerAcknowledgment implements ShareAcknowledgment {

			private final ConsumerRecord<K, V> record;

			private final AtomicReference<@Nullable AcknowledgeType> acknowledgmentType = new AtomicReference<>();

			// Set when the record was redelivered while this delivery was pending; guarded by this
			private boolean retired;

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
				synchronized (this) {
					if (this.retired) {
						ShareListenerConsumer.this.logger.debug(() -> "Discarding " + type
								+ " for a delivery superseded by a redelivery: " + this.record);
						return;
					}
					ShareListenerConsumer.this.acknowledgmentQueue.offer(
							new PendingAcknowledgment<>(this.record, type));
				}
			}

			synchronized void retire() {
				this.retired = true;
			}

			void notifyAcknowledged(AcknowledgeType type) {
				// The authoritative transition already happened in acknowledgeInternal() when the ack was queued.
				// A queued RENEW drained after a terminal ack must not move the state backwards.
				this.acknowledgmentType.updateAndGet(current ->
						current == null || current == AcknowledgeType.RENEW ? type : current);
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
