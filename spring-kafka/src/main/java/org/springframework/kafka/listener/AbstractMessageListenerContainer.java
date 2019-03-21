/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.util.Assert;

/**
 * The base implementation for the {@link MessageListenerContainer}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Marius Bogoevici
 * @author Artem Bilan
 */
public abstract class AbstractMessageListenerContainer<K, V>
		implements MessageListenerContainer, BeanNameAware, ApplicationEventPublisherAware, SmartLifecycle {

	/**
	 * The default {@link SmartLifecycle} phase for listener containers {@value #DEFAULT_PHASE}.
	 */
	public static final int DEFAULT_PHASE = Integer.MAX_VALUE - 100; // late phase

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
		 * User takes responsibility for acks using an
		 * {@link AcknowledgingMessageListener}.
		 */
		MANUAL,

		/**
		 * User takes responsibility for acks using an
		 * {@link AcknowledgingMessageListener}. The consumer
		 * immediately processes the commit.
		 */
		MANUAL_IMMEDIATE,

	}

	private final ContainerProperties containerProperties;

	private final Object lifecycleMonitor = new Object();

	private String beanName;

	private ApplicationEventPublisher applicationEventPublisher;

	private boolean autoStartup = true;

	private int phase = DEFAULT_PHASE;

	private AfterRollbackProcessor<K, V> afterRollbackProcessor = new DefaultAfterRollbackProcessor<>();

	private volatile boolean running = false;

	protected AbstractMessageListenerContainer(ContainerProperties containerProperties) {
		Assert.notNull(containerProperties, "'containerProperties' cannot be null");

		if (containerProperties.getTopics() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopics());
		}
		else if (containerProperties.getTopicPattern() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopicPattern());
		}
		else {
			this.containerProperties = new ContainerProperties(containerProperties.getTopicPartitions());
		}

		BeanUtils.copyProperties(containerProperties, this.containerProperties,
				"topics", "topicPartitions", "topicPattern", "ackCount", "ackTime");

		if (containerProperties.getAckCount() > 0) {
			this.containerProperties.setAckCount(containerProperties.getAckCount());
		}
		if (containerProperties.getAckTime() > 0) {
			this.containerProperties.setAckTime(containerProperties.getAckTime());
		}
		if (this.containerProperties.getConsumerRebalanceListener() == null) {
			this.containerProperties.setConsumerRebalanceListener(createSimpleLoggingConsumerRebalanceListener());
		}
		if (containerProperties.getGenericErrorHandler() instanceof BatchErrorHandler) {
			this.containerProperties.setBatchErrorHandler((BatchErrorHandler) containerProperties.getGenericErrorHandler());
		}
		else {
			this.containerProperties.setErrorHandler((ErrorHandler) containerProperties.getGenericErrorHandler());
		}
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	public ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
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

	protected AfterRollbackProcessor<K, V> getAfterRollbackProcessor() {
		return this.afterRollbackProcessor;
	}

	/**
	 * Set a processor to perform seeks on unprocessed records after a rollback.
	 * Default will seek to current position all topics/partitions, including the failed
	 * record.
	 * @param afterRollbackProcessor the processor.
	 * @since 1.3.5
	 */
	public void setAfterRollbackProcessor(AfterRollbackProcessor<K, V> afterRollbackProcessor) {
		Assert.notNull(afterRollbackProcessor, "'afterRollbackProcessor' cannot be null");
		this.afterRollbackProcessor = afterRollbackProcessor;
	}

	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		this.containerProperties.setMessageListener(messageListener);
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			if (!isRunning()) {
				Assert.isTrue(
						this.containerProperties.getMessageListener() instanceof GenericMessageListener,
						"A " + GenericMessageListener.class.getName() + " implementation must be provided");
				doStart();
			}
		}
	}

	protected abstract void doStart();

	@Override
	public final void stop() {
		synchronized (this.lifecycleMonitor) {
			if (isRunning()) {
				final CountDownLatch latch = new CountDownLatch(1);
				doStop(new Runnable() {

					@Override
					public void run() {
						latch.countDown();
					}
				});
				try {
					latch.await(this.containerProperties.getShutdownTimeout(), TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
				}
			}
		}
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (this.lifecycleMonitor) {
			if (isRunning()) {
				doStop(callback);
			}
		}
	}

	protected abstract void doStop(Runnable callback);

	/**
	 * Return default implementation of {@link ConsumerRebalanceListener} instance.
	 * @return the {@link ConsumerRebalanceListener} currently assigned to this container.
	 */
	protected final ConsumerRebalanceListener createSimpleLoggingConsumerRebalanceListener() {
		return new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				AbstractMessageListenerContainer.this.logger.info("partitions revoked: " + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				AbstractMessageListenerContainer.this.logger.info("partitions assigned: " + partitions);
			}

		};
	}

}
