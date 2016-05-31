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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * The base implementation for the {@link MessageListenerContainer}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Marius Bogoevici
 */
public abstract class AbstractMessageListenerContainer<K, V>
		implements MessageListenerContainer, BeanNameAware, SmartLifecycle {

	private static final int DEFAULT_PAUSE_AFTER = 10000;

	protected final Log logger = LogFactory.getLog(this.getClass()); // NOSONAR

	/**
	 * The offset commit behavior enumeration.
	 */
	public enum AckMode {

		/**
		 * Commit after each record is processed by the listener.
		 */
		RECORD,

		/**
		 * Commit whatever has already been processed before the next poll.
		 */
		BATCH,

		/**
		 * Commit pending updates after
		 * {@link ContainerProperties#setAckTime(long) ackTime} has elapsed.
		 */
		TIME,

		/**
		 * Commit pending updates after
		 * {@link ContainerProperties#setAckCount(int) ackCount} has been
		 * exceeded.
		 */
		COUNT,

		/**
		 * Commit pending updates after
		 * {@link ContainerProperties#setAckCount(int) ackCount} has been
		 * exceeded or after {@link ContainerProperties#setAckTime(long)
		 * ackTime} has elapsed.
		 */
		COUNT_TIME,

		/**
		 * Same as {@link #COUNT_TIME} except for pending manual acks.
		 */
		MANUAL,

		/**
		 * Call {@link Consumer#commitAsync()} immediately for pending acks.
		 */
		MANUAL_IMMEDIATE,

		/**
		 * Call {@link Consumer#commitSync()} immediately for pending acks.
		 */
		MANUAL_IMMEDIATE_SYNC

	}

	private final Object lifecycleMonitor = new Object();

	private String beanName;

	private ContainerProperties containerProperties = new ContainerProperties();

	private boolean autoStartup = true;

	private int phase = 0;

	private volatile boolean running = false;

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	public void setContainerProperties(ContainerProperties containerProperties) {
		Assert.notNull(containerProperties, "'containerProperties' cannot be null");
		this.containerProperties = containerProperties;
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		this.containerProperties.messageListener = messageListener;
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			if (this.containerProperties.recoveryCallback == null) {
				this.containerProperties.recoveryCallback = new RecoveryCallback<Void>() {

					@Override
					public Void recover(RetryContext context) throws Exception {
						@SuppressWarnings("unchecked")
						ConsumerRecord<K, V> record = (ConsumerRecord<K, V>) context.getAttribute("record");
						Throwable lastThrowable = context.getLastThrowable();
						if (AbstractMessageListenerContainer.this.containerProperties.errorHandler != null
								&& lastThrowable instanceof Exception) {
							AbstractMessageListenerContainer.this.containerProperties.errorHandler
									.handle((Exception) lastThrowable, record);
						}
						else {
							AbstractMessageListenerContainer.this.logger.error(
									"Listener threw an exception and no error handler for " + record, lastThrowable);
						}
						return null;
					}

				};
			}
			doStart();
		}
	}

	protected abstract void doStart();

	@Override
	public final void stop() {
		final CountDownLatch latch = new CountDownLatch(1);
		stop(new Runnable() {
			@Override
			public void run() {
				latch.countDown();
			}
		});
		try {
			latch.await(this.containerProperties.shutdownTimeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
		}
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (this.lifecycleMonitor) {
			doStop(callback);
		}
	}

	protected abstract void doStop(Runnable callback);

	/**
	 * Contains runtime properties for a listener container.
	 *
	 */
	public static class ContainerProperties {

		private static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;

		private static final int DEFAULT_QUEUE_DEPTH = 1;

		/**
		 * The ack mode to use when auto ack (in the configuration properties) is false.
		 * <ul>
		 * <li>RECORD: Ack after each record has been passed to the listener.</li>
		 * <li>BATCH: Ack after each batch of records received from the consumer has been
		 * passed to the listener</li>
		 * <li>TIME: Ack after this number of milliseconds; (should be greater than
		 * {@code #setPollTimeout(long) pollTimeout}.</li>
		 * <li>COUNT: Ack after at least this number of records have been received</li>
		 * <li>MANUAL: Listener is responsible for acking - use a
		 * {@link AcknowledgingMessageListener}.
		 * </ul>
		 */
		public AckMode ackMode = AckMode.BATCH;

		/**
		 * The number of outstanding record count after which offsets should be
		 * committed when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being
		 * used.
		 */
		public int ackCount;

		/**
		 * The time (ms) after which outstanding offsets should be committed when
		 * {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should be
		 * larger than
		 */
		public long ackTime;

		/**
		 * The message listener; must be a {@link MessageListener} or
		 * {@link AcknowledgingMessageListener}.
		 */
		public Object messageListener;

		/**
		 * The max time to block in the consumer waiting for records.
		 */
		public volatile long pollTimeout = 1000;

		/**
		 * The executor for threads that poll the consumer.
		 */
		public AsyncListenableTaskExecutor consumerTaskExecutor;

		/**
		 * The executor for threads that invoke the listener.
		 */
		public AsyncListenableTaskExecutor listenerTaskExecutor;

		/**
		 * The error handler to call when the listener throws an exception.
		 */
		public ErrorHandler errorHandler = new LoggingErrorHandler();

		/**
		 * When using Kafka group management and {@link #setPauseEnabled(boolean)} is
		 * true, the delay after which the consumer should be paused. Default 10000.
		 */
		public long pauseAfter = DEFAULT_PAUSE_AFTER;

		/**
		 * When true, avoids rebalancing when this consumer is slow or throws a
		 * qualifying exception - pauses the consumer. Default: true.
		 * @see #pauseAfter
		 */
		public boolean pauseEnabled = true;

		/**
		 * A retry template to retry deliveries.
		 */
		public RetryTemplate retryTemplate;

		/**
		 * A recovery callback to be invoked when retries are exhausted. By default
		 * the error handler is invoked.
		 */
		public RecoveryCallback<Void> recoveryCallback;

		/**
		 * Set the queue depth for handoffs from the consumer thread to the listener
		 * thread. Default 1 (up to 2 in process).
		 */
		public int queueDepth = DEFAULT_QUEUE_DEPTH;

		/**
		 * The timeout for shutting down the container. This is the maximum amount of
		 * time that the invocation to {@link #stop(Runnable)} will block for, before
		 * returning.
		 */
		public long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

		/**
		 * The offset to this number of records back from the latest when starting.
		 * Overrides any consumer properties (earliest, latest). Only applies when
		 * explicit topic/partition assignment is provided.
		 */
		public long recentOffset;

		/**
		 * A user defined {@link ConsumerRebalanceListener} implementation.
		 */
		public ConsumerRebalanceListener consumerRebalanceListener;

		/**
		 * The commit callback; by default a simple logging callback is used to log
		 * success at DEBUG level and failures at ERROR level.
		 */
		public OffsetCommitCallback commitCallback;

		/**
		 * Whether or not to call consumer.commitSync() or commitAsync() when the
		 * container is responsible for commits. Default true. See
		 * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
		 * writing, async commits are not entirely reliable.
		 */
		public boolean syncCommits = true;

		/**
		 * Set the message listener; must be a {@link MessageListener} or
		 * {@link AcknowledgingMessageListener}.
		 * @param messageListener the listener.
		 */
		public void setMessageListener(Object messageListener) {
			Assert.isTrue(
					messageListener instanceof MessageListener
							|| messageListener instanceof AcknowledgingMessageListener,
					"Either a " + MessageListener.class.getName() + " or a "
							+ AcknowledgingMessageListener.class.getName() + " must be provided");
			this.messageListener = messageListener;
		}

		/**
		 * Set the ack mode to use when auto ack (in the configuration properties) is false.
		 * <ul>
		 * <li>RECORD: Ack after each record has been passed to the listener.</li>
		 * <li>BATCH: Ack after each batch of records received from the consumer has been
		 * passed to the listener</li>
		 * <li>TIME: Ack after this number of milliseconds; (should be greater than
		 * {@code #setPollTimeout(long) pollTimeout}.</li>
		 * <li>COUNT: Ack after at least this number of records have been received</li>
		 * <li>MANUAL: Listener is responsible for acking - use a
		 * {@link AcknowledgingMessageListener}.
		 * </ul>
		 * @param ackMode the {@link AckMode}; default BATCH.
		 */
		public void setAckMode(AckMode ackMode) {
			this.ackMode = ackMode;
		}

		/**
		 * Set the max time to block in the consumer waiting for records.
		 * @param pollTimeout the timeout in ms; default 1000.
		 */
		public void setPollTimeout(long pollTimeout) {
			this.pollTimeout = pollTimeout;
		}

		/**
		 * Set the number of outstanding record count after which offsets should be
		 * committed when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being
		 * used.
		 * @param count the count
		 */
		public void setAckCount(int count) {
			this.ackCount = count;
		}

		/**
		 * Return the count.
		 * @return the count.
		 * @see #setAckCount(int)
		 */
		public int getAckCount() {
			return this.ackCount;
		}

		/**
		 * Set the time (ms) after which outstanding offsets should be committed when
		 * {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should be
		 * larger than
		 * @param millis the time
		 */
		public void setAckTime(long millis) {
			this.ackTime = millis;
		}

		/**
		 * Set the error handler to call when the listener throws an exception.
		 * @param errorHandler the error handler.
		 */
		public void setErrorHandler(ErrorHandler errorHandler) {
			this.errorHandler = errorHandler;
		}

		/**
		 * Set the executor for threads that poll the consumer.
		 * @param consumerTaskExecutor the executor
		 */
		public void setConsumerTaskExecutor(AsyncListenableTaskExecutor consumerTaskExecutor) {
			this.consumerTaskExecutor = consumerTaskExecutor;
		}

		/**
		 * Set the executor for threads that invoke the listener.
		 * @param listenerTaskExecutor the executor.
		 */
		public void setListenerTaskExecutor(AsyncListenableTaskExecutor listenerTaskExecutor) {
			this.listenerTaskExecutor = listenerTaskExecutor;
		}

		/**
		 * When using Kafka group management and {@link #setPauseEnabled(boolean)} is
		 * true, set the delay after which the consumer should be paused. Default 10000.
		 * @param pauseAfter the delay.
		 */
		public void setPauseAfter(long pauseAfter) {
			this.pauseAfter = pauseAfter;
		}

		/**
		 * Set to true to avoid rebalancing when this consumer is slow or throws a
		 * qualifying exception - pause the consumer. Default: true.
		 * @param pauseEnabled true to pause.
		 * @see #setPauseAfter(long)
		 */
		public void setPauseEnabled(boolean pauseEnabled) {
			this.pauseEnabled = pauseEnabled;
		}

		/**
		 * Set a retry template to retry deliveries.
		 * @param retryTemplate the retry template.
		 */
		public void setRetryTemplate(RetryTemplate retryTemplate) {
			this.retryTemplate = retryTemplate;
		}

		/**
		 * Set a recovery callback to be invoked when retries are exhausted. By default
		 * the error handler is invoked.
		 * @param recoveryCallback the recovery callback.
		 */
		public void setRecoveryCallback(RecoveryCallback<Void> recoveryCallback) {
			this.recoveryCallback = recoveryCallback;
		}

		/**
		 * Set the queue depth for handoffs from the consumer thread to the listener
		 * thread. Default 1 (up to 2 in process).
		 * @param queueDepth the queue depth.
		 */
		public void setQueueDepth(int queueDepth) {
			this.queueDepth = queueDepth;
		}

		/**
		 * Set the timeout for shutting down the container. This is the maximum amount of
		 * time that the invocation to {@link #stop(Runnable)} will block for, before
		 * returning.
		 * @param shutdownTimeout the shutdown timeout.
		 */
		public void setShutdownTimeout(long shutdownTimeout) {
			this.shutdownTimeout = shutdownTimeout;
		}

		/**
		 * Set the offset to this number of records back from the latest when starting.
		 * Overrides any consumer properties (earliest, latest). Only applies when
		 * explicit topic/partition assignment is provided.
		 * @param recentOffset the offset from the latest; default 0.
		 */
		public void setRecentOffset(long recentOffset) {
			this.recentOffset = recentOffset;
		}

		/**
		 * Set the user defined {@link ConsumerRebalanceListener} implementation.
		 * @param consumerRebalanceListener the {@link ConsumerRebalanceListener} instance
		 */
		public void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
			this.consumerRebalanceListener = consumerRebalanceListener;
		}

		/**
		 * Set the commit callback; by default a simple logging callback is used to log
		 * success at DEBUG level and failures at ERROR level.
		 * @param commitCallback the callback.
		 */
		public void setCommitCallback(OffsetCommitCallback commitCallback) {
			this.commitCallback = commitCallback;
		}

		/**
		 * Set whether or not to call consumer.commitSync() or commitAsync() when the
		 * container is responsible for commits. Default true. See
		 * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
		 * writing, async commits are not entirely reliable.
		 * @param syncCommits true to use commitSync().
		 */
		public void setSyncCommits(boolean syncCommits) {
			this.syncCommits = syncCommits;
		}

	}

}
