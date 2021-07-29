/*
 * Copyright 2018-2021 the original author or authors.
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.util.Assert;
import org.springframework.util.backoff.FixedBackOff;

/**
 *
 * Configures the provided {@link ConcurrentKafkaListenerContainerFactory} with a
 * {@link DefaultErrorHandler}, the {@link DeadLetterPublishingRecoverer} created by
 * the {@link DeadLetterPublishingRecovererFactory}.
 *
 * Mind that the same factory can be used by many different
 * {@link org.springframework.kafka.annotation.RetryableTopic}s but should not be shared
 * with non retryable topics as some of their configurations will be overriden.
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

	private Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer = container -> {
	};

	private Consumer<CommonErrorHandler> errorHandlerCustomizer = errorHandler -> {
	};

	private final DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	private final Clock clock;

	ListenerContainerFactoryConfigurer(KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
									DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory,
									@Qualifier(RetryTopicInternalBeanNames
											.INTERNAL_BACKOFF_CLOCK_BEAN_NAME) Clock clock) {
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.deadLetterPublishingRecovererFactory = deadLetterPublishingRecovererFactory;
		this.clock = clock;
	}

	public ConcurrentKafkaListenerContainerFactory<?, ?> configure(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, Configuration configuration) {
		return isCached(containerFactory)
				? containerFactory
				: addToCache(doConfigure(containerFactory, configuration.backOffValues));
	}

	public ConcurrentKafkaListenerContainerFactory<?, ?> configureWithoutBackOffValues(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, Configuration configuration) {
		return isCached(containerFactory)
				? containerFactory
				: doConfigure(containerFactory, Collections.emptyList());
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> doConfigure(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, List<Long> backOffValues) {

		containerFactory
				.setContainerCustomizer(container -> setupBackoffAwareMessageListenerAdapter(container, backOffValues));
		containerFactory
				.setCommonErrorHandler(createErrorHandler(this.deadLetterPublishingRecovererFactory.create()));
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

	private CommonErrorHandler createErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer,
				new FixedBackOff(0, 0));
		errorHandler.setCommitRecovered(true);
		this.errorHandlerCustomizer.accept(errorHandler);
		return errorHandler;
	}

	private void setupBackoffAwareMessageListenerAdapter(ConcurrentMessageListenerContainer<?, ?> container,
														List<Long> backOffValues) {
		AcknowledgingConsumerAwareMessageListener<?, ?> listener = checkAndCast(container.getContainerProperties()
				.getMessageListener(), AcknowledgingConsumerAwareMessageListener.class);

		configurePollTimeoutAndIdlePartitionInterval(container, backOffValues);

		container.setupMessageListener(new KafkaBackoffAwareMessageListenerAdapter<>(listener,
				this.kafkaConsumerBackoffManager, container.getListenerId(), this.clock)); // NOSONAR

		this.containerCustomizer.accept(container);
	}

	private void configurePollTimeoutAndIdlePartitionInterval(ConcurrentMessageListenerContainer<?, ?> container,
															List<Long> backOffValues) {
		if (backOffValues.isEmpty()) {
			return;
		}

		ContainerProperties containerProperties = container.getContainerProperties();

		long pollTimeoutValue = getPollTimeoutValue(containerProperties, backOffValues);
		long idlePartitionEventInterval = getIdlePartitionInterval(containerProperties, pollTimeoutValue);

		LOGGER.debug(() -> "pollTimeout and idlePartitionEventInterval for back off values "
				+ backOffValues + " will be set to " + pollTimeoutValue
				+ " and " + idlePartitionEventInterval);

		containerProperties
				.setIdlePartitionEventInterval(idlePartitionEventInterval);
		containerProperties.setPollTimeout(pollTimeoutValue);
	}

	private long getIdlePartitionInterval(ContainerProperties containerProperties, long pollTimeoutValue) {
		Long idlePartitionEventInterval = containerProperties.getIdlePartitionEventInterval();
		return idlePartitionEventInterval != null && idlePartitionEventInterval > 0
				? idlePartitionEventInterval
				: pollTimeoutValue;
	}

	private long getPollTimeoutValue(ContainerProperties containerProperties, List<Long> backOffValues) {
		if (containerProperties.getPollTimeout() != ContainerProperties.DEFAULT_POLL_TIMEOUT) {
			return containerProperties.getPollTimeout();
		}

		Long lowestBackOff = backOffValues
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

	static class Configuration {

		private final List<Long> backOffValues;

		Configuration(List<Long> backOffValues) {
			this.backOffValues = backOffValues;
		}
	}
}
