/*
 * Copyright 2016-2021 the original author or authors.
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.lang.Nullable;
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
 * @author Tomaz Fernandes
 */
public abstract class AbstractMessageListenerContainer<K, V>
		implements GenericMessageListenerContainer<K, V>, BeanNameAware, ApplicationEventPublisherAware,
			ApplicationContextAware {

	/**
	 * The default {@link org.springframework.context.SmartLifecycle} phase for listener
	 * containers {@value #DEFAULT_PHASE}.
	 */
	public static final int DEFAULT_PHASE = Integer.MAX_VALUE - 100; // late phase

	private static final int DEFAULT_TOPIC_CHECK_TIMEOUT = 30;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); // NOSONAR

	protected final ConsumerFactory<K, V> consumerFactory; // NOSONAR (final)

	private final ContainerProperties containerProperties;

	protected final Object lifecycleMonitor = new Object(); // NOSONAR

	private String beanName;

	private ApplicationEventPublisher applicationEventPublisher;

	private GenericErrorHandler<?> errorHandler;

	private CommonErrorHandler commonErrorHandler;

	private boolean autoStartup = true;

	private int phase = DEFAULT_PHASE;

	private AfterRollbackProcessor<? super K, ? super V> afterRollbackProcessor =
			new DefaultAfterRollbackProcessor<>();

	private int topicCheckTimeout = DEFAULT_TOPIC_CHECK_TIMEOUT;

	private RecordInterceptor<K, V> recordInterceptor;

	private BatchInterceptor<K, V> batchInterceptor;

	private boolean interceptBeforeTx;

	private volatile boolean running = false;

	private volatile boolean paused;

	private ApplicationContext applicationContext;

	private final Set<TopicPartition> pauseRequestedPartitions = ConcurrentHashMap.newKeySet();

	/**
	 * Construct an instance with the provided factory and properties.
	 * @param consumerFactory the factory.
	 * @param containerProperties the properties.
	 */
	@SuppressWarnings("unchecked")
	protected AbstractMessageListenerContainer(ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties) {

		Assert.notNull(containerProperties, "'containerProperties' cannot be null");
		Assert.notNull(consumerFactory, "'consumerFactory' cannot be null");
		this.consumerFactory = (ConsumerFactory<K, V>) consumerFactory;
		String[] topics = containerProperties.getTopics();
		if (topics != null) {
			this.containerProperties = new ContainerProperties(topics);
		}
		else {
			Pattern topicPattern = containerProperties.getTopicPattern();
			if (topicPattern != null) {
				this.containerProperties = new ContainerProperties(topicPattern);
			}
			else {
				TopicPartitionOffset[] topicPartitions = containerProperties.getTopicPartitions();
				if (topicPartitions != null) {
					this.containerProperties = new ContainerProperties(topicPartitions);
				}
				else {
					throw new IllegalStateException("topics, topicPattern, or topicPartitions must be provided");
				}
			}
		}

		BeanUtils.copyProperties(containerProperties, this.containerProperties,
				"topics", "topicPartitions", "topicPattern", "ackCount", "ackTime", "subBatchPerPartition");

		if (containerProperties.getAckCount() > 0) {
			this.containerProperties.setAckCount(containerProperties.getAckCount());
		}
		if (containerProperties.getAckTime() > 0) {
			this.containerProperties.setAckTime(containerProperties.getAckTime());
		}
		Boolean subBatchPerPartition = containerProperties.getSubBatchPerPartition();
		if (subBatchPerPartition != null) {
			this.containerProperties.setSubBatchPerPartition(subBatchPerPartition);
		}
		if (this.containerProperties.getConsumerRebalanceListener() == null) {
			this.containerProperties.setConsumerRebalanceListener(createSimpleLoggingConsumerRebalanceListener());
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Nullable
	protected ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Return the bean name.
	 * @return the bean name.
	 */
	@Nullable
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
	@Nullable
	public GenericErrorHandler<?> getGenericErrorHandler() {
		return this.errorHandler;
	}

	/**
	 * Get the {@link CommonErrorHandler}.
	 * @return the handler.
	 * @since 2.8
	 */
	@Nullable
	public CommonErrorHandler getCommonErrorHandler() {
		return this.commonErrorHandler;
	}

	/**
	 * Set the {@link CommonErrorHandler} which can handle errors for both record
	 * and batch listeners. Replaces the use of {@link GenericErrorHandler}s.
	 * @param commonErrorHandler the handler.
	 * @since 2.8
	 */
	public void setCommonErrorHandler(@Nullable CommonErrorHandler commonErrorHandler) {
		this.commonErrorHandler = commonErrorHandler;
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
	public boolean isPartitionPauseRequested(TopicPartition topicPartition) {
		return this.pauseRequestedPartitions.contains(topicPartition);
	}

	@Override
	public void pausePartition(TopicPartition topicPartition) {
		this.pauseRequestedPartitions.add(topicPartition);
	}

	@Override
	public void resumePartition(TopicPartition topicPartition) {
		this.pauseRequestedPartitions.remove(topicPartition);
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

	/**
	 * Return the currently configured {@link AfterRollbackProcessor}.
	 * @return the after rollback processor.
	 * @since 2.2.14
	 */
	public AfterRollbackProcessor<? super K, ? super V> getAfterRollbackProcessor() {
		return this.afterRollbackProcessor;
	}

	/**
	 * Set a processor to perform seeks on unprocessed records after a rollback.
	 * Default will seek to current position all topics/partitions, including the failed
	 * record.
	 * @param afterRollbackProcessor the processor.
	 * @since 1.3.5
	 */
	public void setAfterRollbackProcessor(AfterRollbackProcessor<? super K, ? super V> afterRollbackProcessor) {
		Assert.notNull(afterRollbackProcessor, "'afterRollbackProcessor' cannot be null");
		this.afterRollbackProcessor = afterRollbackProcessor;
	}

	@Override
	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	@Nullable
	public String getGroupId() {
		return this.containerProperties.getGroupId() == null
				? (String) this.consumerFactory.getConfigurationProperties().get(ConsumerConfig.GROUP_ID_CONFIG)
				: this.containerProperties.getGroupId();
	}

	@Override
	@Nullable
	public String getListenerId() {
		return this.beanName; // the container factory sets the bean name to the id attribute
	}

	/**
	 * How long to wait for {@link AdminClient#describeTopics(Collection)} result
	 * futures to complete.
	 * @param topicCheckTimeout the timeout in seconds; default 30.
	 * @since 2.3
	 */
	public void setTopicCheckTimeout(int topicCheckTimeout) {
		this.topicCheckTimeout = topicCheckTimeout;
	}

	protected RecordInterceptor<K, V> getRecordInterceptor() {
		return this.recordInterceptor;
	}

	/**
	 * Set an interceptor to be called before calling the record listener.
	 * Does not apply to batch listeners.
	 * @param recordInterceptor the interceptor.
	 * @since 2.2.7
	 * @see #setInterceptBeforeTx(boolean)
	 */
	public void setRecordInterceptor(RecordInterceptor<K, V> recordInterceptor) {
		this.recordInterceptor = recordInterceptor;
	}

	protected BatchInterceptor<K, V> getBatchInterceptor() {
		return this.batchInterceptor;
	}

	/**
	 * Set an interceptor to be called before calling the record listener.
	 * @param batchInterceptor the interceptor.
	 * @since 2.6.6
	 * @see #setInterceptBeforeTx(boolean)
	 */
	public void setBatchInterceptor(BatchInterceptor<K, V> batchInterceptor) {
		this.batchInterceptor = batchInterceptor;
	}

	protected boolean isInterceptBeforeTx() {
		return this.interceptBeforeTx;
	}

	/**
	 * When true, invoke the interceptor before the transaction starts.
	 * @param interceptBeforeTx true to intercept before the transaction.
	 * @since 2.3.4
	 * @see #setRecordInterceptor(RecordInterceptor)
	 * @see #setBatchInterceptor(BatchInterceptor)
	 */
	public void setInterceptBeforeTx(boolean interceptBeforeTx) {
		this.interceptBeforeTx = interceptBeforeTx;
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
				Assert.state(this.containerProperties.getMessageListener() instanceof GenericMessageListener,
						() -> "A " + GenericMessageListener.class.getName() + " implementation must be provided");
				doStart();
			}
		}
	}

	protected void checkTopics() {
		if (this.containerProperties.isMissingTopicsFatal() && this.containerProperties.getTopicPattern() == null) {
			Map<String, Object> configs = this.consumerFactory.getConfigurationProperties()
					.entrySet()
					.stream()
					.filter(entry -> AdminClientConfig.configNames().contains(entry.getKey()))
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
			List<String> missing = null;
			try (AdminClient client = AdminClient.create(configs)) { // NOSONAR - false positive null check
				if (client != null) {
					String[] topics = this.containerProperties.getTopics();
					if (topics == null) {
						topics = Arrays.stream(this.containerProperties.getTopicPartitions())
								.map(TopicPartitionOffset::getTopic)
								.toArray(String[]::new);
					}
					DescribeTopicsResult result = client.describeTopics(Arrays.asList(topics));
					missing = result.values()
							.entrySet()
							.stream()
							.filter(entry -> {
								try {
									entry.getValue().get(this.topicCheckTimeout, TimeUnit.SECONDS);
									return false;
								}
								catch (@SuppressWarnings("unused") Exception e) {
									return true;
								}
							})
							.map(Entry::getKey)
							.collect(Collectors.toList());
				}
			}
			catch (Exception e) {
				this.logger.error(e, "Failed to check topic existence");
			}
			if (missing != null && missing.size() > 0) {
				throw new IllegalStateException(
						"Topic(s) " + missing.toString()
								+ " is/are not present and missingTopicsFatal is true");
			}
		}

	}

	public void checkGroupId() {
		if (this.containerProperties.getTopicPartitions() == null) {
			boolean hasGroupIdConsumerConfig = true; // assume true for non-standard containers
			if (this.consumerFactory != null) { // we always have one for standard containers
				Object groupIdConfig = this.consumerFactory.getConfigurationProperties()
						.get(ConsumerConfig.GROUP_ID_CONFIG);
				hasGroupIdConsumerConfig =
						groupIdConfig instanceof String && StringUtils.hasText((String) groupIdConfig);
			}
			Assert.state(hasGroupIdConsumerConfig || StringUtils.hasText(this.containerProperties.getGroupId()),
					"No group.id found in consumer config, container properties, or @KafkaListener annotation; "
							+ "a group.id is required when group management is used.");
		}
	}

	protected abstract void doStart();

	@Override
	public final void stop() {
		stop(true);
	}

	/**
	 * Stop the container.
	 * @param wait wait for the listener to terminate.
	 * @since 2.3.8
	 */
	public final void stop(boolean wait) {
		synchronized (this.lifecycleMonitor) {
			if (isRunning()) {
				if (wait) {
					final CountDownLatch latch = new CountDownLatch(1);
					doStop(latch::countDown);
					try {
						latch.await(this.containerProperties.getShutdownTimeout(), TimeUnit.MILLISECONDS); // NOSONAR
						publishContainerStoppedEvent();
					}
					catch (@SuppressWarnings("unused") InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				else {
					doStop(() -> {
						publishContainerStoppedEvent();
					});
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
			else {
				callback.run();
			}
		}
	}

	protected abstract void doStop(Runnable callback);

	/**
	 * Return default implementation of {@link ConsumerRebalanceListener} instance.
	 * @return the {@link ConsumerRebalanceListener} currently assigned to this container.
	 */
	protected final ConsumerRebalanceListener createSimpleLoggingConsumerRebalanceListener() {
		return new ConsumerRebalanceListener() { // NOSONAR - anonymous inner class length

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				AbstractMessageListenerContainer.this.logger.info(() ->
						getGroupId() + ": partitions revoked: " + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				AbstractMessageListenerContainer.this.logger.info(() ->
						getGroupId() + ": partitions assigned: " + partitions);
			}

			@Override
			public void onPartitionsLost(Collection<TopicPartition> partitions) {
				AbstractMessageListenerContainer.this.logger.info(() ->
				getGroupId() + ": partitions lost: " + partitions);
			}

		};
	}

	protected void publishContainerStoppedEvent() {
		ApplicationEventPublisher eventPublisher = getApplicationEventPublisher();
		if (eventPublisher != null) {
			eventPublisher.publishEvent(new ContainerStoppedEvent(this, parentOrThis()));
		}
	}

	/**
	 * Return this or a parent container if this has a parent.
	 * @return the parent or this.
	 * @since 2.2.1
	 */
	protected AbstractMessageListenerContainer<?, ?> parentOrThis() {
		return this;
	}

}
