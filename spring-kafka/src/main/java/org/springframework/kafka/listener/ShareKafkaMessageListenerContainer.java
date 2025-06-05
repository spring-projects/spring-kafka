/*
 * Copyright 2016-2025 the original author or authors.
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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.AcknowledgeType;
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
import org.springframework.util.Assert;

/**
 * {@code ShareKafkaMessageListenerContainer} is a message listener container for Kafka's share consumer model.
 * <p>
 * This container manages a single-threaded consumer loop using a {@link org.springframework.kafka.core.ShareConsumerFactory}.
 * It is designed for use cases where Kafka's cooperative sharing protocol is desired, and provides a simple polling loop
 * with per-record dispatch and acknowledgement.
 * <p>
 * Lifecycle events are published for consumer starting and started. The container supports direct setting of the client.id.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 */
public class ShareKafkaMessageListenerContainer<K, V>
		extends AbstractShareKafkaMessageListenerContainer<K, V> {

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
	 * Set the {@code client.id} to use for the consumer.
	 * @param clientId the client id to set
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * Get the {@code client.id} for the consumer.
	 * @return the client id, or null if not set
	 */
	@Nullable
	public String getClientId() {
		return this.clientId;
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
	 * The inner share consumer thread: polls for records and dispatches to the listener.
	 */
	private class ShareListenerConsumer implements Runnable {

		private final LogAccessor logger = ShareKafkaMessageListenerContainer.this.logger;

		private final ShareConsumer<K, V> consumer;

		private final GenericMessageListener<?> genericListener;

		private final @Nullable String consumerGroupId = ShareKafkaMessageListenerContainer.this.getGroupId();

		private final @Nullable String clientId;

		ShareListenerConsumer(GenericMessageListener<?> listener) {
			this.consumer = ShareKafkaMessageListenerContainer.this.shareConsumerFactory.createShareConsumer(
				ShareKafkaMessageListenerContainer.this.getGroupId(),
				ShareKafkaMessageListenerContainer.this.getClientId());
			this.genericListener = listener;
			this.clientId = ShareKafkaMessageListenerContainer.this.getClientId();
			// Subscribe to topics, just like in the test
			ContainerProperties containerProperties = getContainerProperties();
			this.consumer.subscribe(java.util.Arrays.asList(containerProperties.getTopics()));
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
					var records = this.consumer.poll(java.time.Duration.ofMillis(1000));
					if (records != null && records.count() > 0) {
						for (var record : records) {
							@SuppressWarnings("unchecked")
							GenericMessageListener<Object> listener = (GenericMessageListener<Object>) this.genericListener;
							listener.onMessage(record);
							// Temporarily auto-acknowledge and commit.
							// We will refactor it later on to support more production-like scenarios.
							this.consumer.acknowledge(record, AcknowledgeType.ACCEPT);
							this.consumer.commitSync();
						}
					}
				}
				catch (Error e) {
					this.logger.error(e, "Stopping share consumer due to an Error");
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
			this.consumer.close();
			this.logger.info(() -> this.consumerGroupId + ": Consumer stopped");
		}

		protected void initialize() {
			publishConsumerStartingEvent();
			publishConsumerStartedEvent();
		}

		@Override
		public String toString() {
			return "ShareKafkaMessageListenerContainer.ShareListenerConsumer ["
				+ "consumerGroupId=" + this.consumerGroupId
				+ ", clientId=" + this.clientId
				+ "]";
		}
	}
}
