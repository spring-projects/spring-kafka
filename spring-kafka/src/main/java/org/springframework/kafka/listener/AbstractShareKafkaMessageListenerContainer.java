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

import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;

/**
 * Abstract base class for share consumer message listener containers.
 * <p>
 * Handles common lifecycle, configuration, and event publishing for containers using a
 * {@link org.springframework.kafka.core.ShareConsumerFactory}.
 * <p>
 * Subclasses are responsible for implementing the actual consumer loop and message dispatch logic.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 * @since 4.0
 */
public abstract class AbstractShareKafkaMessageListenerContainer<K, V>
		implements GenericMessageListenerContainer<K, V>, BeanNameAware, ApplicationEventPublisherAware,
		ApplicationContextAware {

	/**
	 * The default {@link org.springframework.context.SmartLifecycle} phase for listener containers.
	 */
	public static final int DEFAULT_PHASE = Integer.MAX_VALUE - 100;

	/**
	 * The share consumer factory used to create consumer instances.
	 */
	protected final ShareConsumerFactory<K, V> shareConsumerFactory;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final ContainerProperties containerProperties;

	protected final ReentrantLock lifecycleLock = new ReentrantLock();

	private String beanName = "noBeanNameSet";

	@Nullable
	private ApplicationEventPublisher applicationEventPublisher;

	private boolean autoStartup = true;

	private int phase = DEFAULT_PHASE;

	@Nullable
	private ApplicationContext applicationContext;

	private volatile boolean running = false;

	/**
	 * Construct an instance with the provided factory and properties.
	 * @param shareConsumerFactory the factory.
	 * @param containerProperties the properties.
	 */
	@SuppressWarnings("unchecked")
	protected AbstractShareKafkaMessageListenerContainer(ShareConsumerFactory<? super K, ? super V> shareConsumerFactory,
														ContainerProperties containerProperties) {
		Assert.notNull(containerProperties, "'containerProperties' cannot be null");
		Assert.notNull(shareConsumerFactory, "'shareConsumerFactory' cannot be null");
		this.shareConsumerFactory = (ShareConsumerFactory<K, V>) shareConsumerFactory;
		String @Nullable [] topics = containerProperties.getTopics();
		if (topics != null) {
			this.containerProperties = new ContainerProperties(topics);
		}
		else {
			Pattern topicPattern = containerProperties.getTopicPattern();
			if (topicPattern != null) {
				this.containerProperties = new ContainerProperties(topicPattern);
			}
			else {
				TopicPartitionOffset @Nullable [] topicPartitions = containerProperties.getTopicPartitions();
				if (topicPartitions != null) {
					this.containerProperties = new ContainerProperties(topicPartitions);
				}
				else {
					throw new IllegalStateException("topics, topicPattern, or topicPartitions must be provided");
				}
			}
		}
		BeanUtils.copyProperties(containerProperties, this.containerProperties,
				"topics", "topicPartitions", "topicPattern");
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Nullable
	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Return the bean name.
	 * @return the bean name
	 */
	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/**
	 * Get the event publisher.
	 * @return the publisher
	 */
	@Nullable
	public ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public void start() {
		this.lifecycleLock.lock();
		try {
			if (!isRunning()) {
				Assert.state(this.containerProperties.getMessageListener() instanceof GenericMessageListener,
						() -> "A " + GenericMessageListener.class.getName() + " implementation must be provided");
				doStart();
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public void stop() {
		this.lifecycleLock.lock();
		try {
			if (isRunning()) {
				doStop();
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	@Nullable
	public String getGroupId() {
		return this.containerProperties.getGroupId() == null
				? (String) this.shareConsumerFactory.getConfigurationProperties().get(ConsumerConfig.GROUP_ID_CONFIG)
				: this.containerProperties.getGroupId();
	}

	@Override
	public String getListenerId() {
		return this.beanName; // the container factory sets the bean name to the id attribute
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		this.containerProperties.setMessageListener(messageListener);
	}

	protected abstract void doStart();

	protected abstract void doStop();

	@Override
	public void destroy() {
		stop();
	}
}
