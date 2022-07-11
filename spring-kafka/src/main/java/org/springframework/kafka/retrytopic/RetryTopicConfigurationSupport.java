/*
 * Copyright 2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerPartitionPausingBackOffManagerFactory;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.kafka.listener.KafkaBackOffManagerFactory;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * This is the main class providing the configuration behind the non-blocking,
 * topic-based delayed retries feature. It is typically imported by adding
 * {@link EnableKafkaRetryTopic @EnableKafkaRetryTopic} to an application
 * {@link Configuration @Configuration} class. An alternative more advanced option
 * is to extend directly from this class and override methods as necessary, remembering
 * to add {@link Configuration @Configuration} to the subclass and {@link Bean @Bean}
 * to overridden {@link Bean @Bean} methods. For more details see the javadoc of
 * {@link EnableKafkaRetryTopic @EnableRetryTopic}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.9
*/
public class RetryTopicConfigurationSupport {

	private static final AtomicBoolean ONLY_ONE_ALLOWED = new AtomicBoolean(true);

	private final RetryTopicComponentFactory componentFactory = createComponentFactory();

	public RetryTopicConfigurationSupport() {
		Assert.state(ONLY_ONE_ALLOWED.getAndSet(false), "Only one 'RetryTopicConfigurationSupport' is allowed");
	}

	/**
	 * Return a global {@link RetryTopicConfigurer} for configuring retry topics
	 * for {@link KafkaListenerEndpoint} instances with a corresponding
	 * {@link org.springframework.kafka.retrytopic.RetryTopicConfiguration}.
	 * To configure it, consider overriding the {@link #configureRetryTopicConfigurer()}.
	 * @param kafkaConsumerBackoffManager the global {@link KafkaConsumerBackoffManager}.
	 * @param destinationTopicResolver the global {@link DestinationTopicResolver}.
	 * @param beanFactory the {@link BeanFactory}.
	 * @return the instance.
	 * @see KafkaListenerAnnotationBeanPostProcessor
	 */
	@Bean(name = RetryTopicBeanNames.RETRY_TOPIC_CONFIGURER_BEAN_NAME)
	public RetryTopicConfigurer retryTopicConfigurer(@Qualifier(KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME)
			KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
			@Qualifier(RetryTopicBeanNames.DESTINATION_TOPIC_RESOLVER_BEAN_NAME)
			DestinationTopicResolver destinationTopicResolver,
			BeanFactory beanFactory) {

		DestinationTopicProcessor destinationTopicProcessor = this.componentFactory
				.destinationTopicProcessor(destinationTopicResolver);
		DeadLetterPublishingRecovererFactory dlprf = this.componentFactory
				.deadLetterPublishingRecovererFactory(destinationTopicResolver);
		ListenerContainerFactoryConfigurer lcfc = this.componentFactory
				.listenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						dlprf, this.componentFactory.internalRetryTopicClock());
		ListenerContainerFactoryResolver factoryResolver = this.componentFactory
				.listenerContainerFactoryResolver(beanFactory);
		RetryTopicNamesProviderFactory retryTopicNamesProviderFactory =
				this.componentFactory.retryTopicNamesProviderFactory();

		processDeadLetterPublishingContainerFactory(dlprf);
		processListenerContainerFactoryConfigurer(lcfc);

		RetryTopicConfigurer retryTopicConfigurer = this.componentFactory
				.retryTopicConfigurer(destinationTopicProcessor, lcfc,
						factoryResolver, retryTopicNamesProviderFactory);

		Consumer<RetryTopicConfigurer> configurerConsumer = configureRetryTopicConfigurer();
		Assert.notNull(configurerConsumer, "configureRetryTopicConfigurer cannot return null.");
		configurerConsumer.accept(retryTopicConfigurer);
		return retryTopicConfigurer;
	}

	/**
	 * Override this method if you need to configure the {@link RetryTopicConfigurer}.
	 * @return a {@link RetryTopicConfigurer} consumer.
	 */
	protected Consumer<RetryTopicConfigurer> configureRetryTopicConfigurer() {
		return retryTopicConfigurer -> {
		};
	}

	/**
	 * Internal method for processing the {@link DeadLetterPublishingRecovererFactory}.
	 * Consider overriding the {@link #configureDeadLetterPublishingContainerFactory()}
	 * method if further customization is required.
	 * @param deadLetterPublishingRecovererFactory the instance.
	 */
	private void processDeadLetterPublishingContainerFactory(
			DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory) {

		CustomizersConfigurer customizersConfigurer = new CustomizersConfigurer();
		configureCustomizers(customizersConfigurer);
		JavaUtils.INSTANCE
				.acceptIfNotNull(customizersConfigurer.deadLetterPublishingRecovererCustomizer,
						deadLetterPublishingRecovererFactory::setDeadLetterPublishingRecovererCustomizer);
		Consumer<DeadLetterPublishingRecovererFactory> dlprfConsumer = configureDeadLetterPublishingContainerFactory();
		Assert.notNull(dlprfConsumer, "configureDeadLetterPublishingContainerFactory must not return null");
		dlprfConsumer.accept(deadLetterPublishingRecovererFactory);
	}

	/**
	 * Override this method to further configure the {@link DeadLetterPublishingRecovererFactory}.
	 * @return a {@link DeadLetterPublishingRecovererFactory} consumer.
	 */
	protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
		return dlprf -> {
		};
	}

	/**
	 * Internal method for processing the {@link ListenerContainerFactoryConfigurer}.
	 * Consider overriding {@link #configureListenerContainerFactoryConfigurer()}
	 * if further customization is required.
	 * @param listenerContainerFactoryConfigurer the {@link ListenerContainerFactoryConfigurer} instance.
	 */
	private void processListenerContainerFactoryConfigurer(ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer) {
		CustomizersConfigurer customizersConfigurer = new CustomizersConfigurer();
		configureCustomizers(customizersConfigurer);
		BlockingRetriesConfigurer blockingRetriesConfigurer = new BlockingRetriesConfigurer();
		configureBlockingRetries(blockingRetriesConfigurer);
		JavaUtils.INSTANCE
				.acceptIfNotNull(blockingRetriesConfigurer.backOff,
						listenerContainerFactoryConfigurer::setBlockingRetriesBackOff)
				.acceptIfNotNull(blockingRetriesConfigurer.retryableExceptions,
						listenerContainerFactoryConfigurer::setBlockingRetryableExceptions)
				.acceptIfNotNull(customizersConfigurer.errorHandlerCustomizer,
						listenerContainerFactoryConfigurer::setErrorHandlerCustomizer)
				.acceptIfNotNull(customizersConfigurer.listenerContainerCustomizer,
						listenerContainerFactoryConfigurer::setContainerCustomizer);
		Consumer<ListenerContainerFactoryConfigurer> lcfcConfigurer = configureListenerContainerFactoryConfigurer();
		Assert.notNull(lcfcConfigurer, "configureListenerContainerFactoryConfigurer must not return null.");
		lcfcConfigurer.accept(listenerContainerFactoryConfigurer);
	}

	/**
	 * Override this method to further configure the {@link ListenerContainerFactoryConfigurer}.
	 * @return a {@link ListenerContainerFactoryConfigurer} consumer.
	 */
	protected Consumer<ListenerContainerFactoryConfigurer> configureListenerContainerFactoryConfigurer() {
		return lcfc -> {
		};
	}

	/**
	 * Override this method to configure blocking retries parameters
	 * such as exceptions to be retried and the {@link BackOff} to be used.
	 * @param blockingRetries a {@link BlockingRetriesConfigurer}.
	 */
	protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
	}

	/**
	 * Override this method to manage non-blocking retries fatal exceptions.
	 * Records which processing throws an exception present in this list will be
	 * forwarded directly to the DLT, if one is configured, or stop being processed
	 * otherwise.
	 * @param nonBlockingRetriesExceptions a {@link List} of fatal exceptions
	 * containing the framework defaults.
	 */
	protected void manageNonBlockingFatalExceptions(List<Class<? extends Throwable>> nonBlockingRetriesExceptions) {
	}

	/**
	 * Override this method to configure customizers for components created
	 * by non-blocking retries' configuration, such as {@link MessageListenerContainer},
	 * {@link DeadLetterPublishingRecoverer} and {@link DefaultErrorHandler}.
	 * @param customizersConfigurer a {@link CustomizersConfigurer}.
	 */
	protected void configureCustomizers(CustomizersConfigurer customizersConfigurer) {
	}

	/**
	 * Return a global {@link DestinationTopicResolver} for resolving
	 * the {@link DestinationTopic} to which a given {@link ConsumerRecord}
	 * should be sent for retry.
	 *
	 * To configure it, consider overriding one of these other more
	 * fine-grained methods:
	 * <ul>
	 * <li>{@link #manageNonBlockingFatalExceptions} to configure non-blocking retries.
	 * <li>{@link #configureDestinationTopicResolver} to further customize the component.
	 * <li>{@link #createComponentFactory} to provide a subclass instance.
	 * </ul>
	 * @return the instance.
	 */
	@Bean(name = RetryTopicBeanNames.DESTINATION_TOPIC_RESOLVER_BEAN_NAME)
	public DestinationTopicResolver destinationTopicResolver() {
		DestinationTopicResolver destinationTopicResolver = this.componentFactory.destinationTopicResolver();
		JavaUtils.INSTANCE.acceptIfInstanceOf(DefaultDestinationTopicResolver.class, destinationTopicResolver,
				this::configureNonBlockingFatalExceptions);
		Consumer<DestinationTopicResolver> resolverConsumer = configureDestinationTopicResolver();
		Assert.notNull(resolverConsumer, "customizeDestinationTopicResolver must not return null");
		resolverConsumer.accept(destinationTopicResolver);
		return destinationTopicResolver;
	}

	private void configureNonBlockingFatalExceptions(DefaultDestinationTopicResolver destinationTopicResolver) {
		List<Class<? extends Throwable>> fatalExceptions =
				new ArrayList<>(ExceptionClassifier.defaultFatalExceptionsList());
		manageNonBlockingFatalExceptions(fatalExceptions);
		destinationTopicResolver.setClassifications(fatalExceptions.stream()
				.collect(Collectors.toMap(ex -> ex, ex -> false)), true);
	}

	/**
	 * Override this method to configure the {@link DestinationTopicResolver}.
	 * @return a {@link DestinationTopicResolver} consumer.
	 */
	protected Consumer<DestinationTopicResolver> configureDestinationTopicResolver() {
		return dtr -> {
		};
	}

	/**
	 * Create the {@link KafkaConsumerBackoffManager} instance that will be used to
	 * back off partitions.
	 * To provide a custom implementation, either override this method, or
	 * override the {@link RetryTopicComponentFactory#kafkaBackOffManagerFactory} method
	 * and return a different {@link KafkaBackOffManagerFactory}.
	 * @param applicationContext the application context.
	 * @param registry the {@link ListenerContainerRegistry} to be used to fetch the
	 * {@link MessageListenerContainer} at runtime to be backed off.
	 * @param wrapper a {@link RetryTopicSchedulerWrapper}.
	 * @param taskScheduler a {@link TaskScheduler}.
	 * @return the instance.
	 */
	@Bean(name = KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME)
	public KafkaConsumerBackoffManager kafkaConsumerBackoffManager(ApplicationContext applicationContext,
			@Qualifier(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
					ListenerContainerRegistry registry, @Nullable RetryTopicSchedulerWrapper wrapper,
					@Nullable TaskScheduler taskScheduler) {

		KafkaBackOffManagerFactory backOffManagerFactory =
				this.componentFactory.kafkaBackOffManagerFactory(registry, applicationContext);
		JavaUtils.INSTANCE.acceptIfInstanceOf(ContainerPartitionPausingBackOffManagerFactory.class, backOffManagerFactory,
				factory -> configurePartitionPausingFactory(factory, registry,
						wrapper != null ? wrapper.getScheduler() : taskScheduler));
		return backOffManagerFactory.create();
	}

	private void configurePartitionPausingFactory(ContainerPartitionPausingBackOffManagerFactory factory,
			ListenerContainerRegistry registry, @Nullable TaskScheduler scheduler) {

		Assert.notNull(scheduler, "Either a RetryTopicSchedulerWrapper or TaskScheduler bean is required");
		factory.setBackOffHandler(new ContainerPausingBackOffHandler(
				new ListenerContainerPauseService(registry, scheduler)));
	}

	/**
	 * Override this method to provide a subclass of {@link RetryTopicComponentFactory}
	 * with different component implementations or subclasses.
	 * @return the instance.
	 */
	protected RetryTopicComponentFactory createComponentFactory() {
		return new RetryTopicComponentFactory();
	}

	/**
	 * Configure blocking retries to be used along non-blocking.
	 */
	public static class BlockingRetriesConfigurer {

		private BackOff backOff;

		private Class<? extends Exception>[] retryableExceptions;

		/**
		 * Set the exceptions that should be retried by the blocking retry mechanism.
		 * @param exceptions the exceptions.
		 * @return the configurer.
		 * @see DefaultErrorHandler
		 */
		@SuppressWarnings("varargs")
		@SafeVarargs
		public final BlockingRetriesConfigurer retryOn(Class<? extends Exception>... exceptions) {
			this.retryableExceptions = Arrays.copyOf(exceptions, exceptions.length);
			return this;
		}

		/**
		 * Set the {@link BackOff} that should be used with the blocking retry mechanism.
		 * By default, a {@link FixedBackOff} with 0 delay and 9 retry attempts
		 * is configured. Note that this only has any effect for exceptions specified
		 * with the {@link #retryOn} method - by default blocking retries are disabled
		 * when using the non-blocking retries feature.
		 * @param backoff the {@link BackOff} instance.
		 * @return the configurer.
		 * @see DefaultErrorHandler
		 */
		public BlockingRetriesConfigurer backOff(BackOff backoff) {
			this.backOff = backoff;
			return this;
		}
	}

	/**
	 * Configure customizers for components instantiated by the retry topics feature.
	 */
	public static class CustomizersConfigurer {

		private Consumer<DefaultErrorHandler> errorHandlerCustomizer;

		private Consumer<ConcurrentMessageListenerContainer<?, ?>> listenerContainerCustomizer;

		private Consumer<DeadLetterPublishingRecoverer> deadLetterPublishingRecovererCustomizer;

		/**
		 * Customize the {@link CommonErrorHandler} instances that will be used for the
		 * feature.
		 * @param errorHandlerCustomizer the customizer.
		 * @return the configurer.
		 * @see DefaultErrorHandler
		 */
		public CustomizersConfigurer customizeErrorHandler(Consumer<DefaultErrorHandler> errorHandlerCustomizer) {
			this.errorHandlerCustomizer = errorHandlerCustomizer;
			return this;
		}

		/**
		 * Customize the {@link ConcurrentMessageListenerContainer} instances created
		 * for the retry and DLT consumers.
		 * @param listenerContainerCustomizer the customizer.
		 * @return the configurer.
		 */
		public CustomizersConfigurer customizeListenerContainer(Consumer<ConcurrentMessageListenerContainer<?, ?>> listenerContainerCustomizer) {
			this.listenerContainerCustomizer = listenerContainerCustomizer;
			return this;
		}

		/**
		 * Customize the {@link DeadLetterPublishingRecoverer} that will be used to
		 * forward the records to the retry topics and DLT.
		 * @param dlprCustomizer the customizer.
		 * @return the configurer.
		 */
		public CustomizersConfigurer customizeDeadLetterPublishingRecoverer(Consumer<DeadLetterPublishingRecoverer> dlprCustomizer) {
			this.deadLetterPublishingRecovererCustomizer = dlprCustomizer;
			return this;
		}
	}

}
