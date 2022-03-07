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

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.Consumer;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.lang.Nullable;


/**
 *
 * <p>Configures main, retry and DLT topics based on a main endpoint and provided
 * configurations to acomplish a distributed retry / DLT pattern in a non-blocking
 * fashion, at the expense of ordering guarantees.
 *
 * <p>To illustrate, if you have a "main-topic" topic, and want an exponential backoff
 * of 1000ms with a multiplier of 2 and 3 retry attempts, it will create the
 * main-topic-retry-1000, main-topic-retry-2000, main-topic-retry-4000 and main-topic-dlt
 * topics. The configuration can be achieved using a {@link RetryTopicConfigurationBuilder}
 * to create one or more {@link RetryTopicConfigurer} beans, or by using the
 * {@link org.springframework.kafka.annotation.RetryableTopic} annotation.
 * More details on usage below.
 *
 *
 * <p>How it works:
 *
 * <p>If a message processing throws an exception, the configured
 * {@link org.springframework.kafka.listener.SeekToCurrentErrorHandler}
 * and {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer} forwards the message to the next topic, using a
 * {@link org.springframework.kafka.retrytopic.DestinationTopicResolver}
 * to know the next topic and the delay for it.
 *
 * <p>Each forwareded record has a back off timestamp header and, if consumption is
 * attempted by the {@link org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter}
 * before that time, the partition consumption is paused by a
 * {@link org.springframework.kafka.listener.KafkaConsumerBackoffManager} and a
 * {@link org.springframework.kafka.listener.KafkaBackoffException} is thrown.
 *
 * <p>When the partition has been idle for the amount of time specified in the
 * ContainerProperties' idlePartitionEventInterval property.
 * property, a {@link org.springframework.kafka.event.ListenerContainerPartitionIdleEvent}
 * is published, which the {@link org.springframework.kafka.listener.KafkaConsumerBackoffManager}
 * listens to in order to check whether or not it should unpause the partition.
 *
 * <p>If, when consumption is resumed, processing fails again, the message is forwarded to
 * the next topic and so on, until it gets to the dlt.
 *
 * <p>Considering Kafka's partition ordering guarantees, and each topic having a fixed
 * delay time, we know that the first message consumed in a given retry topic partition will
 * be the one with the earliest backoff timestamp for that partition, so by pausing the
 * partition we know we're not delaying message processing in other partitions longer than
 * necessary.
 *
 *
 * <p>Usages:
 *
 * <p>There are two main ways for configuring the endpoints. The first is by providing one or more
 * {@link org.springframework.context.annotation.Bean}s in a {@link org.springframework.context.annotation.Configuration}
 * annotated class, such as:
 *
 * <pre>
 *     <code>@Bean</code>
 *     <code>public RetryTopicConfiguration myRetryableTopic(KafkaTemplate&lt;String, Object&gt; template) {
 *         return RetryTopicConfigurationBuilder
 *                 .newInstance()
 *                 .create(template);
 *      }</code>
 * </pre>
 * <p>This will create retry and dlt topics for all topics in methods annotated with
 * {@link org.springframework.kafka.annotation.KafkaListener}, as well as its consumers,
 * using the default configurations. If message processing fails it will forward the message
 * to the next topic until it gets to the DLT topic.
 *
 * A {@link org.springframework.kafka.core.KafkaOperations} instance is required for message forwarding.
 *
 * <p>For more fine-grained control over how to handle retrials for each topic, more then one bean can be provided, such as:
 *
 * <pre>
 *     <code>@Bean
 *     public RetryTopicConfiguration myRetryableTopic(KafkaTemplate&lt;String, MyPojo&gt; template) {
 *         return RetryTopicConfigurationBuilder
 *                 .newInstance()
 *                 .fixedBackoff(3000)
 *                 .maxAttempts(5)
 *                 .includeTopics("my-topic", "my-other-topic")
 *                 .create(template);
 *         }</code>
 * </pre>
 * <pre>
 *	   <code>@Bean
 *     public RetryTopicConfiguration myOtherRetryableTopic(KafkaTemplate&lt;String, MyPojo&gt; template) {
 *         return RetryTopicConfigurationBuilder
 *                 .newInstance()
 *                 .exponentialBackoff(1000, 2, 5000)
 *                 .maxAttempts(4)
 *                 .excludeTopics("my-topic", "my-other-topic")
 *                 .retryOn(MyException.class)
 *                 .create(template);
 *         }</code>
 * </pre>
 * <p>Some other options include: auto-creation of topics, backoff,
 * retryOn / notRetryOn / transversing as in {@link org.springframework.retry.support.RetryTemplate},
 * single-topic fixed backoff processing, custom dlt listener beans, custom topic
 * suffixes and providing specific listenerContainerFactories.
 *
 * <p>The other, non-exclusive way to configure the endpoints is through the convenient
 * {@link org.springframework.kafka.annotation.RetryableTopic} annotation, that can be placed on any
 * {@link org.springframework.kafka.annotation.KafkaListener} annotated methods, such as:
 *
 * <pre>
 *     <code>@RetryableTopic(attempts = 3,
 *     		backoff = @Backoff(delay = 700, maxDelay = 12000, multiplier = 3))</code>
 *     <code>@KafkaListener(topics = "my-annotated-topic")
 *     public void processMessage(MyPojo message) {
 *        		// ... message processing
 *     }</code>
 *</pre>
 * <p> The same configurations are available in the annotation and the builder approaches, and both can be
 * used concurrently. In case the same method / topic can be handled by both, the annotation takes precedence.
 *
 * <p>DLT Handling:
 *
 * <p>The DLT handler method can be provided through the
 * {@link RetryTopicConfigurationBuilder#dltHandlerMethod(Class, String)} method,
 * providing the class and method name that should handle the DLT topic. If a bean
 * instance of this type is found in the {@link BeanFactory} it is the instance used.
 * If not an instance is created. The class can use dependency injection as a normal bean.
 *
 * <pre>
 *     <code>@Bean
 *     public RetryTopicConfiguration otherRetryTopic(KafkaTemplate&lt;Integer, MyPojo&gt; template) {
 *         return RetryTopicConfigurationBuilder
 *                 .newInstance()
 *                 .dltProcessor(MyCustomDltProcessor.class, "processDltMessage")
 *                 .create(template);
 *     }</code>
 *
 *     <code>@Component
 *     public class MyCustomDltProcessor {
 *
 *     		public void processDltMessage(MyPojo message) {
 *  	       // ... message processing, persistence, etc
 *     		}
 *     }</code>
 * </pre>
 *
 * The other way to provide the DLT handler method is through the
 * {@link org.springframework.kafka.annotation.DltHandler} annotation,
 * that should be used within the same class as the correspondent
 * {@link org.springframework.kafka.annotation.KafkaListener}.
 *
 * 	<pre>
 * 	    <code>@DltHandler
 *       public void processMessage(MyPojo message) {
 *          		// ... message processing, persistence, etc
 *       }</code>
 *</pre>
 *
 * If no DLT handler is provided, the default {@link LoggingDltListenerHandlerMethod} is used.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 * @see RetryTopicConfigurationBuilder
 * @see org.springframework.kafka.annotation.RetryableTopic
 * @see org.springframework.kafka.annotation.KafkaListener
 * @see org.springframework.retry.annotation.Backoff
 * @see org.springframework.kafka.listener.SeekToCurrentErrorHandler
 * @see org.springframework.kafka.listener.DeadLetterPublishingRecoverer
 *
 */
public class RetryTopicConfigurer {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(RetryTopicConfigurer.class));

	/**
	 * The default method to handle messages in the DLT.
	 */
	public static final EndpointHandlerMethod DEFAULT_DLT_HANDLER = createHandlerMethodWith(LoggingDltListenerHandlerMethod.class,
			LoggingDltListenerHandlerMethod.DEFAULT_DLT_METHOD_NAME);

	private final DestinationTopicProcessor destinationTopicProcessor;

	private final ListenerContainerFactoryResolver containerFactoryResolver;

	private final ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer;

	private final BeanFactory beanFactory;

	private final RetryTopicNamesProviderFactory retryTopicNamesProviderFactory;

	private boolean useLegacyFactoryConfigurer = false;

	@Deprecated
	public RetryTopicConfigurer(DestinationTopicProcessor destinationTopicProcessor,
								ListenerContainerFactoryResolver containerFactoryResolver,
								ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer,
								BeanFactory beanFactory) {

		this(destinationTopicProcessor, containerFactoryResolver, listenerContainerFactoryConfigurer, beanFactory, new SuffixingRetryTopicNamesProviderFactory());
	}

	/**
	 * Create an instance with the provided properties.
	 * @param destinationTopicProcessor the destination topic processor.
	 * @param containerFactoryResolver the container factory resolver.
	 * @param listenerContainerFactoryConfigurer the container factory configurer.
	 * @param beanFactory the bean factory.
	 * @param retryTopicNamesProviderFactory the retry topic names factory.
	 */
	@Autowired
	public RetryTopicConfigurer(DestinationTopicProcessor destinationTopicProcessor,
								ListenerContainerFactoryResolver containerFactoryResolver,
								ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer,
								BeanFactory beanFactory,
								RetryTopicNamesProviderFactory retryTopicNamesProviderFactory) {

		this.destinationTopicProcessor = destinationTopicProcessor;
		this.containerFactoryResolver = containerFactoryResolver;
		this.listenerContainerFactoryConfigurer = listenerContainerFactoryConfigurer;
		this.beanFactory = beanFactory;
		this.retryTopicNamesProviderFactory = retryTopicNamesProviderFactory;
	}

	/**
	 * Entrypoint for creating and configuring the retry and dlt endpoints, as well as the
	 * container factory that will create the corresponding listenerContainer.
	 * @param endpointProcessor function that will process the endpoints
	 * processListener method.
	 * @param mainEndpoint the endpoint based on which retry and dlt endpoints are also
	 * created and processed.
	 * @param configuration the configuration for the topic.
	 * @param registrar The {@link KafkaListenerEndpointRegistrar} that will register the endpoints.
	 * @param factory The factory provided in the {@link org.springframework.kafka.annotation.KafkaListener}
	 * @param defaultContainerFactoryBeanName The default factory bean name for the
	 * {@link org.springframework.kafka.annotation.KafkaListener}
	 *
	 */
	public void processMainAndRetryListeners(EndpointProcessor endpointProcessor,
											MethodKafkaListenerEndpoint<?, ?> mainEndpoint,
											RetryTopicConfiguration configuration,
											KafkaListenerEndpointRegistrar registrar,
											@Nullable KafkaListenerContainerFactory<?> factory,
											String defaultContainerFactoryBeanName) {
		throwIfMultiMethodEndpoint(mainEndpoint);
		DestinationTopicProcessor.Context context =
				new DestinationTopicProcessor.Context(configuration.getDestinationTopicProperties());
		configureEndpoints(mainEndpoint, endpointProcessor, factory, registrar, configuration, context,
				defaultContainerFactoryBeanName);
		this.destinationTopicProcessor.processRegisteredDestinations(getTopicCreationFunction(configuration), context);
	}

	private void configureEndpoints(MethodKafkaListenerEndpoint<?, ?> mainEndpoint,
									EndpointProcessor endpointProcessor,
									KafkaListenerContainerFactory<?> factory,
									KafkaListenerEndpointRegistrar registrar,
									RetryTopicConfiguration configuration,
									DestinationTopicProcessor.Context context,
									String defaultContainerFactoryBeanName) {
		this.destinationTopicProcessor
				.processDestinationTopicProperties(destinationTopicProperties ->
						processAndRegisterEndpoint(mainEndpoint,
								endpointProcessor,
								factory,
								defaultContainerFactoryBeanName,
								registrar,
								configuration,
								context,
								destinationTopicProperties),
						context);
	}

	private void processAndRegisterEndpoint(MethodKafkaListenerEndpoint<?, ?> mainEndpoint, EndpointProcessor endpointProcessor,
											KafkaListenerContainerFactory<?> factory,
											String defaultFactoryBeanName,
											KafkaListenerEndpointRegistrar registrar,
											RetryTopicConfiguration configuration, DestinationTopicProcessor.Context context,
											DestinationTopic.Properties destinationTopicProperties) {

		KafkaListenerContainerFactory<?> resolvedFactory =
				destinationTopicProperties.isMainEndpoint()
						? resolveAndConfigureFactoryForMainEndpoint(factory, defaultFactoryBeanName, configuration)
						: resolveAndConfigureFactoryForRetryEndpoint(factory, defaultFactoryBeanName, configuration);

		MethodKafkaListenerEndpoint<?, ?> endpoint = destinationTopicProperties.isMainEndpoint()
				? mainEndpoint
				: new MethodKafkaListenerEndpoint<>();

		endpointProcessor.accept(endpoint);

		EndpointHandlerMethod endpointBeanMethod =
				getEndpointHandlerMethod(mainEndpoint, configuration, destinationTopicProperties);

		createEndpointCustomizer(endpointBeanMethod, destinationTopicProperties)
						.customizeEndpointAndCollectTopics(endpoint)
						.forEach(topicNamesHolder ->
								this.destinationTopicProcessor
										.registerDestinationTopic(topicNamesHolder.getMainTopic(),
												topicNamesHolder.getCustomizedTopic(),
												destinationTopicProperties, context));

		registrar.registerEndpoint(endpoint, resolvedFactory);
		endpoint.setBeanFactory(this.beanFactory);
	}

	protected EndpointHandlerMethod getEndpointHandlerMethod(MethodKafkaListenerEndpoint<?, ?> mainEndpoint,
														RetryTopicConfiguration configuration,
														DestinationTopic.Properties props) {
		EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		EndpointHandlerMethod retryBeanMethod = new EndpointHandlerMethod(mainEndpoint.getBean(), mainEndpoint.getMethod());
		return props.isDltTopic() ? getDltEndpointHandlerMethodOrDefault(dltHandlerMethod) : retryBeanMethod;
	}

	private Consumer<Collection<String>> getTopicCreationFunction(RetryTopicConfiguration config) {
		RetryTopicConfiguration.TopicCreation topicCreationConfig = config.forKafkaTopicAutoCreation();
		return topicCreationConfig.shouldCreateTopics()
				? topics -> createNewTopicBeans(topics, topicCreationConfig)
				: topics -> { };
	}

	protected void createNewTopicBeans(Collection<String> topics, RetryTopicConfiguration.TopicCreation config) {
		topics.forEach(topic ->
				((DefaultListableBeanFactory) this.beanFactory)
						.registerSingleton(topic + "-topicRegistrationBean",
								new NewTopic(topic, config.getNumPartitions(), config.getReplicationFactor()))
		);
	}

	protected EndpointCustomizer createEndpointCustomizer(
			EndpointHandlerMethod endpointBeanMethod, DestinationTopic.Properties destinationTopicProperties) {

		return new EndpointCustomizerFactory(destinationTopicProperties,
				endpointBeanMethod,
				this.beanFactory,
				this.retryTopicNamesProviderFactory)
				.createEndpointCustomizer();
	}

	private EndpointHandlerMethod getDltEndpointHandlerMethodOrDefault(EndpointHandlerMethod dltEndpointHandlerMethod) {
		return dltEndpointHandlerMethod != null ? dltEndpointHandlerMethod : DEFAULT_DLT_HANDLER;
	}

	@SuppressWarnings("deprecation")
	private KafkaListenerContainerFactory<?> resolveAndConfigureFactoryForMainEndpoint(
			KafkaListenerContainerFactory<?> providedFactory,
			String defaultFactoryBeanName, RetryTopicConfiguration configuration) {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = this.containerFactoryResolver
				.resolveFactoryForMainEndpoint(providedFactory, defaultFactoryBeanName,
						configuration.forContainerFactoryResolver());

		return this.useLegacyFactoryConfigurer
				? this.listenerContainerFactoryConfigurer
				.configureWithoutBackOffValues(resolvedFactory, configuration.forContainerFactoryConfigurer())
				: this.listenerContainerFactoryConfigurer
					.decorateFactoryWithoutSettingContainerProperties(resolvedFactory,
							configuration.forContainerFactoryConfigurer());
	}

	@SuppressWarnings("deprecation")
	private KafkaListenerContainerFactory<?> resolveAndConfigureFactoryForRetryEndpoint(
			KafkaListenerContainerFactory<?> providedFactory,
			String defaultFactoryBeanName,
			RetryTopicConfiguration configuration) {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				this.containerFactoryResolver.resolveFactoryForRetryEndpoint(providedFactory, defaultFactoryBeanName,
				configuration.forContainerFactoryResolver());
		return this.useLegacyFactoryConfigurer
				? this.listenerContainerFactoryConfigurer.configure(resolvedFactory,
					configuration.forContainerFactoryConfigurer())
				: this.listenerContainerFactoryConfigurer
					.decorateFactory(resolvedFactory, configuration.forContainerFactoryConfigurer());
	}

	private void throwIfMultiMethodEndpoint(MethodKafkaListenerEndpoint<?, ?> mainEndpoint) {
		if (mainEndpoint instanceof MultiMethodKafkaListenerEndpoint) {
			throw new IllegalArgumentException("Retry Topic is not compatible with " + MultiMethodKafkaListenerEndpoint.class);
		}
	}

	public static EndpointHandlerMethod createHandlerMethodWith(Object beanOrClass, String methodName) {
		return new EndpointHandlerMethod(beanOrClass, methodName);
	}

	public static EndpointHandlerMethod createHandlerMethodWith(Object bean, Method method) {
		return new EndpointHandlerMethod(bean, method);
	}

	/**
	 * Set to true if you want the {@link ListenerContainerFactoryConfigurer} to
	 * behave as before 2.8.3.
	 * @param useLegacyFactoryConfigurer Whether to use the legacy factory configuration.
	 * @deprecated for removal after the deprecated legacy configuration methods are removed.
	 */
	@Deprecated
	public void useLegacyFactoryConfigurer(boolean useLegacyFactoryConfigurer) {
		this.useLegacyFactoryConfigurer = useLegacyFactoryConfigurer;
	}

	public interface EndpointProcessor extends Consumer<MethodKafkaListenerEndpoint<?, ?>> {

		default void process(MethodKafkaListenerEndpoint<?, ?> listenerEndpoint) {
			accept(listenerEndpoint);
		}
	}

	static class LoggingDltListenerHandlerMethod {

		public static final String DEFAULT_DLT_METHOD_NAME = "logMessage";

		@SuppressWarnings("deprecation")
		public void logMessage(Object message) {
			if (message instanceof ConsumerRecord) {
				LOGGER.info(() -> "Received message in dlt listener: "
						+ ListenerUtils.recordToString((ConsumerRecord<?, ?>) message));
			}
			else {
				LOGGER.info(() -> "Received message in dlt listener.");
			}
		}
	}

}
