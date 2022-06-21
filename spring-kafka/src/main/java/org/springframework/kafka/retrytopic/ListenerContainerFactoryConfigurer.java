/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.time.Clock;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;

/**
 *
 * Decorates the provided {@link ConcurrentKafkaListenerContainerFactory} to add a
 * {@link DefaultErrorHandler} and the {@link DeadLetterPublishingRecoverer}
 * created by the {@link DeadLetterPublishingRecovererFactory}.
 *
 * Also sets {@link ContainerProperties#setIdlePartitionEventInterval(Long)}
 * and {@link ContainerProperties#setPollTimeout(long)} if its defaults haven't
 * been overridden by the user.
 *
 * Since 2.8.3 these configurations don't interfere with the provided factory
 * instance itself, so the same factory instance can be shared among retryable and
 * non-retryable endpoints.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class ListenerContainerFactoryConfigurer {

	private static final Set<ConcurrentKafkaListenerContainerFactory<?, ?>> CONFIGURED_FACTORIES_CACHE;

	private static final LogAccessor LOGGER = new LogAccessor(
			LogFactory.getLog(ListenerContainerFactoryConfigurer.class));

	static {
		CONFIGURED_FACTORIES_CACHE = new HashSet<>();
	}

	private static final int MIN_POLL_TIMEOUT_VALUE = 100;

	private static final int MAX_POLL_TIMEOUT_VALUE = 5000;

	private static final int POLL_TIMEOUT_DIVISOR = 4;

	private static final long LOWEST_BACKOFF_THRESHOLD = 1500L;

	private BackOff providedBlockingBackOff = null;

	private Class<? extends Exception>[] blockingExceptionTypes = null;

	private Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer = container -> {
	};

	private Consumer<CommonErrorHandler> errorHandlerCustomizer = errorHandler -> {
	};

	private final DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	private final Clock clock;

	public ListenerContainerFactoryConfigurer(KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
									DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory,
									@Qualifier("internalBackOffClock") Clock clock) {
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.deadLetterPublishingRecovererFactory = deadLetterPublishingRecovererFactory;
		this.clock = clock;
	}

	/**
	 * Configures the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * @param containerFactory the factory instance to be configured.
	 * @param configuration the configuration provided by the {@link RetryTopicConfiguration}.
	 * @return the configured factory instance.
	 * @deprecated in favor of
	 * {@link #decorateFactory(ConcurrentKafkaListenerContainerFactory, Configuration)}.
	 */
	@Deprecated
	public ConcurrentKafkaListenerContainerFactory<?, ?> configure(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, Configuration configuration) {
		return isCached(containerFactory)
				? containerFactory
				: addToCache(doConfigure(containerFactory, configuration, true));
	}

	/**
	 * Configures the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * Meant to be used for the main endpoint, this method ignores the provided backOff values.
	 * @param containerFactory the factory instance to be configured.
	 * @param configuration the configuration provided by the {@link RetryTopicConfiguration}.
	 * @return the configured factory instance.
	 * @deprecated in favor of
	 * {@link #decorateFactoryWithoutSettingContainerProperties(ConcurrentKafkaListenerContainerFactory, Configuration)}.
	 */
	@Deprecated
	public ConcurrentKafkaListenerContainerFactory<?, ?> configureWithoutBackOffValues(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, Configuration configuration) {
		return isCached(containerFactory)
				? containerFactory
				: doConfigure(containerFactory, configuration, false);
	}

	/**
	 * Decorates the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * @param factory the factory instance to be decorated.
	 * @param configuration the configuration provided by the {@link RetryTopicConfiguration}.
	 * @return the decorated factory instance.
	 */
	public KafkaListenerContainerFactory<?> decorateFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory,
															Configuration configuration) {
		return new RetryTopicListenerContainerFactoryDecorator(factory, configuration, true);
	}

	/**
	 * Decorates the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * Meant to be used for the main endpoint, this method ignores the provided backOff values.
	 * @param factory the factory instance to be decorated.
	 * @param configuration the configuration provided by the {@link RetryTopicConfiguration}.
	 * @return the decorated factory instance.
	 */
	public KafkaListenerContainerFactory<?> decorateFactoryWithoutSettingContainerProperties(
			ConcurrentKafkaListenerContainerFactory<?, ?> factory, Configuration configuration) {
		return new RetryTopicListenerContainerFactoryDecorator(factory, configuration, false);
	}

	/**
	 * Set a {@link BackOff} to be used with blocking retries.
	 * If the BackOff execution returns STOP, the record will be forwarded
	 * to the next retry topic or to the DLT, depending on how the non-blocking retries
	 * are configured.
	 * @param blockingBackOff the BackOff policy to be used by blocking retries.
	 * @since 2.8.4
	 * @see DefaultErrorHandler
	 */
	public void setBlockingRetriesBackOff(BackOff blockingBackOff) {
		Assert.notNull(blockingBackOff, "The provided BackOff cannot be null");
		Assert.state(this.providedBlockingBackOff == null, () ->
				"Blocking retries back off has already been set. Current: "
						+ this.providedBlockingBackOff
						+ " You provided: " + blockingBackOff);
		this.providedBlockingBackOff = blockingBackOff;
	}

	/**
	 * Specify the exceptions to be retried via blocking.
	 * @param exceptionTypes the exceptions that should be retried.
	 * @since 2.8.4
	 * @see DefaultErrorHandler
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public final void setBlockingRetryableExceptions(Class<? extends Exception>... exceptionTypes) {
		Assert.notNull(exceptionTypes, "The exception types cannot be null");
		Assert.noNullElements(exceptionTypes, "The exception types cannot have null elements");
		Assert.state(this.blockingExceptionTypes == null,
				() -> "Blocking retryable exceptions have already been set."
						+  "Current ones: " + Arrays.toString(this.blockingExceptionTypes)
						+ " You provided: " + Arrays.toString(exceptionTypes));
		this.blockingExceptionTypes = Arrays.copyOf(exceptionTypes, exceptionTypes.length);
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> doConfigure(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, Configuration configuration,
			boolean isSetContainerProperties) {

		containerFactory
				.setContainerCustomizer(container -> setupBackoffAwareMessageListenerAdapter(container, configuration, isSetContainerProperties));
		containerFactory
				.setCommonErrorHandler(createErrorHandler(this.deadLetterPublishingRecovererFactory.create(), configuration));
		return containerFactory;
	}

	private boolean isCached(ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory) {
		synchronized (CONFIGURED_FACTORIES_CACHE) {
			return CONFIGURED_FACTORIES_CACHE.contains(containerFactory);
		}
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> addToCache(ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory) {
		synchronized (CONFIGURED_FACTORIES_CACHE) {
			CONFIGURED_FACTORIES_CACHE.add(containerFactory);
			return containerFactory;
		}
	}

	public void setContainerCustomizer(Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer) {
		Assert.notNull(containerCustomizer, "'containerCustomizer' cannot be null");
		this.containerCustomizer = containerCustomizer;
	}

	public void setErrorHandlerCustomizer(Consumer<CommonErrorHandler> errorHandlerCustomizer) {
		this.errorHandlerCustomizer = errorHandlerCustomizer;
	}

	protected CommonErrorHandler createErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer,
												Configuration configuration) {
		DefaultErrorHandler errorHandler = createDefaultErrorHandlerInstance(deadLetterPublishingRecoverer);
		errorHandler.defaultFalse();
		errorHandler.setCommitRecovered(true);
		errorHandler.setLogLevel(KafkaException.Level.DEBUG);
		if (this.blockingExceptionTypes != null) {
			errorHandler.addRetryableExceptions(this.blockingExceptionTypes);
		}
		this.errorHandlerCustomizer.accept(errorHandler);
		return errorHandler;
	}

	protected DefaultErrorHandler createDefaultErrorHandlerInstance(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		return this.providedBlockingBackOff != null
				? new DefaultErrorHandler(deadLetterPublishingRecoverer, this.providedBlockingBackOff)
				: new DefaultErrorHandler(deadLetterPublishingRecoverer);
	}

	protected void setupBackoffAwareMessageListenerAdapter(ConcurrentMessageListenerContainer<?, ?> container,
														Configuration configuration, boolean isSetContainerProperties) {
		MessageListener<?, ?> listener = checkAndCast(container.getContainerProperties()
				.getMessageListener(), MessageListener.class);

		if (isSetContainerProperties && !configuration.backOffValues.isEmpty()) {
			configurePollTimeoutAndIdlePartitionInterval(container, configuration);
		}

		container.setupMessageListener(new KafkaBackoffAwareMessageListenerAdapter<>(listener,
				this.kafkaConsumerBackoffManager, container.getListenerId(), this.clock)); // NOSONAR

		this.containerCustomizer.accept(container);
	}

	protected void configurePollTimeoutAndIdlePartitionInterval(ConcurrentMessageListenerContainer<?, ?> container,
															Configuration configuration) {

		ContainerProperties containerProperties = container.getContainerProperties();

		long pollTimeoutValue = getPollTimeoutValue(containerProperties, configuration);
		long idlePartitionEventInterval = getIdlePartitionInterval(containerProperties, pollTimeoutValue);

		LOGGER.debug(() -> "pollTimeout and idlePartitionEventInterval for back off values "
				+ configuration.backOffValues + " will be set to " + pollTimeoutValue
				+ " and " + idlePartitionEventInterval);

		containerProperties
				.setIdlePartitionEventInterval(idlePartitionEventInterval);
		containerProperties.setPollTimeout(pollTimeoutValue);
	}

	protected long getIdlePartitionInterval(ContainerProperties containerProperties, long pollTimeoutValue) {
		Long idlePartitionEventInterval = containerProperties.getIdlePartitionEventInterval();
		return idlePartitionEventInterval != null && idlePartitionEventInterval > 0
				? idlePartitionEventInterval
				: pollTimeoutValue;
	}

	protected long getPollTimeoutValue(ContainerProperties containerProperties, Configuration configuration) {
		if (containerProperties.getPollTimeout() != ContainerProperties.DEFAULT_POLL_TIMEOUT
				|| configuration.backOffValues.isEmpty()) {
			return containerProperties.getPollTimeout();
		}

		Long lowestBackOff = configuration.backOffValues
				.stream()
				.min(Comparator.naturalOrder())
				.orElseThrow(() -> new IllegalArgumentException("No back off values found!"));

		return lowestBackOff > LOWEST_BACKOFF_THRESHOLD
				? applyLimits(lowestBackOff / POLL_TIMEOUT_DIVISOR)
				: MIN_POLL_TIMEOUT_VALUE;
	}

	private long applyLimits(long pollTimeoutValue) {
		return Math.min(Math.max(pollTimeoutValue, MIN_POLL_TIMEOUT_VALUE), MAX_POLL_TIMEOUT_VALUE);
	}

	@SuppressWarnings("unchecked")
	private <T> T checkAndCast(Object obj, Class<T> clazz) {
		Assert.isAssignable(clazz, obj.getClass(),
				() -> String.format("The provided class %s is not assignable from %s",
						obj.getClass().getSimpleName(), clazz.getSimpleName()));
		return (T) obj;
	}

	private class RetryTopicListenerContainerFactoryDecorator
			implements KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<?, ?>> {

		private final ConcurrentKafkaListenerContainerFactory<?, ?> delegate;
		private final Configuration configuration;
		private final boolean isSetContainerProperties;

		RetryTopicListenerContainerFactoryDecorator(ConcurrentKafkaListenerContainerFactory<?, ?> delegate,
														Configuration configuration,
														boolean isSetContainerProperties) {
			this.delegate = delegate;
			this.configuration = configuration;
			this.isSetContainerProperties = isSetContainerProperties;
		}

		@Override
		public ConcurrentMessageListenerContainer<?, ?> createListenerContainer(KafkaListenerEndpoint endpoint) {
			return decorate(this.delegate.createListenerContainer(endpoint));
		}

		private ConcurrentMessageListenerContainer<?, ?> decorate(ConcurrentMessageListenerContainer<?, ?> listenerContainer) {
			listenerContainer
					.setCommonErrorHandler(createErrorHandler(
							ListenerContainerFactoryConfigurer.this.deadLetterPublishingRecovererFactory.create(),
							this.configuration));
			setupBackoffAwareMessageListenerAdapter(listenerContainer, this.configuration, this.isSetContainerProperties);
			return listenerContainer;
		}

		@Override
		public ConcurrentMessageListenerContainer<?, ?> createContainer(TopicPartitionOffset... topicPartitions) {
			return decorate(this.delegate.createContainer(topicPartitions));
		}

		@Override
		public ConcurrentMessageListenerContainer<?, ?> createContainer(String... topics) {
			return decorate(this.delegate.createContainer(topics));
		}

		@Override
		public ConcurrentMessageListenerContainer<?, ?> createContainer(Pattern topicPattern) {
			return decorate(this.delegate.createContainer(topicPattern));
		}
	}

	static class Configuration {

		private final List<Long> backOffValues;

		Configuration(List<Long> backOffValues) {
			this.backOffValues = backOffValues;
		}
	}
}
