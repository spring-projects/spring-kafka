/*
 * Copyright 2018-present the original author or authors.
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
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
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
 * Since 2.8.3 these configurations don't interfere with the provided factory
 * instance itself, so the same factory instance can be shared among retryable and
 * non-retryable endpoints.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @since 2.7
 *
 */
public class ListenerContainerFactoryConfigurer {

	@Nullable
	private BackOff providedBlockingBackOff;

	private Class<? extends Exception> @Nullable [] blockingExceptionTypes;

	private boolean retainStandardFatal;

	private Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer = container -> {
	};

	private @Nullable Consumer<DefaultErrorHandler> errorHandlerCustomizer = errorHandler -> {
	};

	private final DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	private final Clock clock;

	public ListenerContainerFactoryConfigurer(KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
									DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory,
									Clock clock) {
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.deadLetterPublishingRecovererFactory = deadLetterPublishingRecovererFactory;
		this.clock = clock;
	}

	/**
	 * Decorates the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * @param factory the factory instance to be decorated.
	 * @param configuration the configuration provided by the {@link RetryTopicConfiguration}.
	 * @return the decorated factory instance.
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	public KafkaListenerContainerFactory<?> decorateFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory,
															Configuration configuration) {
		return this.decorateFactory(factory);
	}

	/**
	 * Decorates the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * Meant to be used for the main endpoint, this method ignores the provided backOff values.
	 * @param factory the factory instance to be decorated.
	 * @param configuration the configuration provided by the {@link RetryTopicConfiguration}.
	 * @return the decorated factory instance.
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	public KafkaListenerContainerFactory<?> decorateFactoryWithoutSettingContainerProperties(
			ConcurrentKafkaListenerContainerFactory<?, ?> factory, Configuration configuration) {
		return this.decorateFactory(factory);
	}

	/**
	 * Decorates the provided {@link ConcurrentKafkaListenerContainerFactory}.
	 * @param factory the factory instance to be decorated.
	 * @return the decorated factory instance.
	 * @since 3.2
	 */
	public KafkaListenerContainerFactory<?> decorateFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
		return new RetryTopicListenerContainerFactoryDecorator(factory);
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
	public void setBlockingRetriesBackOff(@Nullable BackOff blockingBackOff) {
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

	/**
	 * Set to true to retain standard fatal exceptions as not retryable when configuring
	 * blocking retries.
	 * @param retainStandardFatal true to retain standard fatal exceptions.
	 * @since 3.0
	 */
	public void setRetainStandardFatal(boolean retainStandardFatal) {
		this.retainStandardFatal = retainStandardFatal;
	}

	public void setContainerCustomizer(@Nullable Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer) {
		Assert.notNull(containerCustomizer, "'containerCustomizer' cannot be null");
		this.containerCustomizer = containerCustomizer;
	}

	public void setErrorHandlerCustomizer(@Nullable Consumer<DefaultErrorHandler> errorHandlerCustomizer) {
		this.errorHandlerCustomizer = errorHandlerCustomizer;
	}

	protected CommonErrorHandler createErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		DefaultErrorHandler errorHandler = createDefaultErrorHandlerInstance(deadLetterPublishingRecoverer);
		errorHandler.defaultFalse(this.retainStandardFatal);
		errorHandler.setCommitRecovered(true);
		errorHandler.setLogLevel(KafkaException.Level.DEBUG);
		if (this.blockingExceptionTypes != null) {
			errorHandler.addRetryableExceptions(this.blockingExceptionTypes);
		}
		if (this.errorHandlerCustomizer != null) {
			this.errorHandlerCustomizer.accept(errorHandler);
		}
		return errorHandler;
	}

	protected DefaultErrorHandler createDefaultErrorHandlerInstance(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		return this.providedBlockingBackOff != null
				? new DefaultErrorHandler(deadLetterPublishingRecoverer, this.providedBlockingBackOff)
				: new DefaultErrorHandler(deadLetterPublishingRecoverer);
	}

	protected void setupBackoffAwareMessageListenerAdapter(ConcurrentMessageListenerContainer<?, ?> container) {
		MessageListener<?, ?> listener = checkAndCast(container.getContainerProperties()
				.getMessageListener(), MessageListener.class);

		container.setupMessageListener(new KafkaBackoffAwareMessageListenerAdapter<>(listener,
				this.kafkaConsumerBackoffManager, container.getListenerId(), this.clock)); // NOSONAR

		this.containerCustomizer.accept(container);
	}

	@SuppressWarnings("unchecked")
	private <T> T checkAndCast(@Nullable Object obj, Class<T> clazz) {
		Assert.state(obj != null, "Object cannot be null");
		Assert.isAssignable(clazz, obj.getClass(),
				() -> String.format("The provided class %s is not assignable from %s",
						obj.getClass().getSimpleName(), clazz.getSimpleName()));
		return (T) obj;
	}

	private class RetryTopicListenerContainerFactoryDecorator
			implements KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<?, ?>> {

		private final ConcurrentKafkaListenerContainerFactory<?, ?> delegate;

		RetryTopicListenerContainerFactoryDecorator(ConcurrentKafkaListenerContainerFactory<?, ?> delegate) {
			this.delegate = delegate;
		}

		@Override
		public ConcurrentMessageListenerContainer<?, ?> createListenerContainer(KafkaListenerEndpoint endpoint) {
			return decorate(this.delegate.createListenerContainer(endpoint));
		}

		private ConcurrentMessageListenerContainer<?, ?> decorate(
				ConcurrentMessageListenerContainer<?, ?> listenerContainer) {

			String mainListenerId = listenerContainer.getMainListenerId();
			if (mainListenerId == null) {
				mainListenerId = listenerContainer.getListenerId();
			}
			CommonErrorHandler errorHandler = createErrorHandler(
					ListenerContainerFactoryConfigurer.this.deadLetterPublishingRecovererFactory
							.create(mainListenerId));
			if (listenerContainer.getContainerProperties().isAsyncAcks()) {
				AckMode ackMode = listenerContainer.getContainerProperties().getAckMode();
				if ((AckMode.MANUAL.equals(ackMode) || AckMode.MANUAL_IMMEDIATE.equals(ackMode))
						&& errorHandler instanceof DefaultErrorHandler deh) {
					deh.setSeekAfterError(false);
				}
			}
			listenerContainer
					.setCommonErrorHandler(errorHandler);
			setupBackoffAwareMessageListenerAdapter(listenerContainer);
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

	@Deprecated(since = "3.2", forRemoval = true)
	static class Configuration {

		private final List<Long> backOffValues;

		Configuration(List<Long> backOffValues) {
			this.backOffValues = backOffValues;
		}
	}
}
