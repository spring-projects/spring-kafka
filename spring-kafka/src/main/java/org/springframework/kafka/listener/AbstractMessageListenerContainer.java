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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
		implements GenericMessageListenerContainer<K, V>, BeanNameAware, ApplicationEventPublisherAware {

	/**
	 * The default {@link org.springframework.context.SmartLifecycle} phase for listener
	 * containers {@value #DEFAULT_PHASE}.
	 */
	public static final int DEFAULT_PHASE = Integer.MAX_VALUE - 100; // late phase

	protected final Log logger = LogFactory.getLog(this.getClass()); // NOSONAR

	protected final ConsumerFactory<K, V> consumerFactory; // NOSONAR (final)

	private final ContainerProperties containerProperties;

	private final Object lifecycleMonitor = new Object();

	private String beanName;

	private ApplicationEventPublisher applicationEventPublisher;

	private GenericErrorHandler<?> errorHandler;

	private boolean autoStartup = true;

	private int phase = DEFAULT_PHASE;

	private AfterRollbackProcessor<K, V> afterRollbackProcessor = new DefaultAfterRollbackProcessor<>();

	private volatile boolean running = false;

	private volatile boolean paused;

	/**
	 * Construct an instance with the provided properties.
	 * @param containerProperties the properties.
	 * @deprecated in favor of
	 * {@link #AbstractMessageListenerContainer(ConsumerFactory, ContainerProperties)}.
	 */
	@Deprecated
	protected AbstractMessageListenerContainer(ContainerProperties containerProperties) {
		this(null, containerProperties);
	}

	/**
	 * Construct an instance with the provided factory and properties.
	 * @param consumerFactory the factory.
	 * @param containerProperties the properties.
	 */
	protected AbstractMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties) {

		Assert.notNull(containerProperties, "'containerProperties' cannot be null");
		this.consumerFactory = consumerFactory;
		if (containerProperties.getTopics() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopics());
		}
		else if (containerProperties.getTopicPattern() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopicPattern());
		}
		else if (containerProperties.getTopicPartitions() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopicPartitions());
		}
		else {
			throw new IllegalStateException("topics, topicPattern, or topicPartitions must be provided");
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

	/**
	 * Set the error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 * @since 2.2
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 * @since 2.2
	 */
	public void setGenericErrorHandler(GenericErrorHandler<?> errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the batch error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 * @since 2.2
	 */
	public void setBatchErrorHandler(BatchErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Get the configured error handler.
	 * @return the error handler.
	 * @since 2.2
	 */
	protected GenericErrorHandler<?> getGenericErrorHandler() {
		return this.errorHandler;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
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

	protected boolean isPaused() {
		return this.paused;
	}

	@Override
	public boolean isPauseRequested() {
		return this.paused;
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

	@Override
	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		this.containerProperties.setMessageListener(messageListener);
	}

	@Override
	public final void start() {
		checkGroupId();
		synchronized (this.lifecycleMonitor) {
			if (!isRunning()) {
				Assert.isTrue(
						this.containerProperties.getMessageListener() instanceof GenericMessageListener,
						"A " + GenericMessageListener.class.getName() + " implementation must be provided");
				doStart();
			}
		}
	}

	protected void checkTopics() {
		if (this.containerProperties.isMissingTopicsFatal() && this.containerProperties.getTopicPattern() == null) {
			try (Consumer<K, V> consumer = this.consumerFactory.createConsumer(this.containerProperties.getGroupId(),
					this.containerProperties.getClientId(), null)) {
				if (consumer != null) {
					String[] topics = this.containerProperties.getTopics();
					if (topics == null) {
						topics = Arrays.stream(this.containerProperties.getTopicPartitions())
								.map(tp -> tp.topic())
								.toArray(String[]::new);
					}
					List<String> missing = new ArrayList<>();
					for (String topic : topics) {
						if (consumer.partitionsFor(topic) == null) {
							missing.add(topic);
						}
					}
					if (missing.size() > 0) {
						throw new IllegalStateException(
								"Topic(s) " + missing.toString()
										+ " is/are not present and missingTopicsFatal is true");
					}
				}
			}
		}
	}

	public void checkGroupId() {
		if (this.containerProperties.getTopicPartitions() == null) {
			boolean hasGroupIdConsumerConfig = true; // assume true for non-standard containers
			if (this.consumerFactory != null) { // we always have one for standard containers
				Object groupIdConfig = this.consumerFactory.getConfigurationProperties()
						.get(ConsumerConfig.GROUP_ID_CONFIG);
				hasGroupIdConsumerConfig = groupIdConfig != null && groupIdConfig instanceof String
						&& StringUtils.hasText((String) groupIdConfig);
			}
			Assert.state(hasGroupIdConsumerConfig || StringUtils.hasText(this.containerProperties.getGroupId()),
					"No group.id found in consumer config, container properties, or @KafkaListener annotation; "
					+ "a group.id is required when group management is used.");
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
					latch.await(this.containerProperties.getShutdownTimeout(), TimeUnit.MILLISECONDS); // NOSONAR
					publishContainerStoppedEvent();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	@Override
	public void pause() {
		this.paused = true;
	}

	@Override
	public void resume() {
		this.paused = false;
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (this.lifecycleMonitor) {
			if (isRunning()) {
				doStop(callback);
				publishContainerStoppedEvent();
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

	protected void publishContainerStoppedEvent() {
		if (getApplicationEventPublisher() != null) {
			getApplicationEventPublisher().publishEvent(new ContainerStoppedEvent(this, parentOrThis()));
		}
	}

	/**
	 * Ruturn this or a parent container if this has a parent.
	 * @return the parent or this.
	 * @since 2.2.1
	 */
	protected AbstractMessageListenerContainer<?, ?> parentOrThis() {
		return this;
	}

}
