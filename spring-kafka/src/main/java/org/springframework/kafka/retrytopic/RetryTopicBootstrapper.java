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
import java.util.function.Supplier;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.listener.KafkaBackOffManagerFactory;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.KafkaConsumerTimingAdjuster;
import org.springframework.kafka.listener.PartitionPausingBackOffManagerFactory;
import org.springframework.kafka.listener.PartitionPausingBackoffManager;
import org.springframework.retry.backoff.ThreadWaitSleeper;

/**
 *
 * Bootstraps the {@link RetryTopicConfigurer} context, registering the dependency
 * beans and configuring the {@link org.springframework.context.ApplicationListener}s.
 *
 * Note that if a bean with the same name already exists in the context that one will
 * be used instead.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @deprecated in favor of {@link org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport}
 *
 */
@Deprecated
public class RetryTopicBootstrapper {

	private final ApplicationContext applicationContext;

	private final BeanFactory beanFactory;

	public RetryTopicBootstrapper(ApplicationContext applicationContext, BeanFactory beanFactory) {
		if (!ConfigurableApplicationContext.class.isAssignableFrom(applicationContext.getClass()) ||
				!BeanDefinitionRegistry.class.isAssignableFrom(applicationContext.getClass())) {
			throw new IllegalStateException(String.format("ApplicationContext must be implement %s and %s interfaces. Provided: %s",
					ConfigurableApplicationContext.class.getSimpleName(),
					BeanDefinitionRegistry.class.getSimpleName(),
					applicationContext.getClass().getSimpleName()));
		}
		if (!SingletonBeanRegistry.class.isAssignableFrom(beanFactory.getClass())) {
			throw new IllegalStateException("BeanFactory must implement " + SingletonBeanRegistry.class +
					" interface. Provided: " + beanFactory.getClass().getSimpleName());
		}
		this.beanFactory = beanFactory;
		this.applicationContext = applicationContext;
	}

	public void bootstrapRetryTopic() {
		registerBeans();
		registerSingletons();
		addApplicationListeners();
	}

	private void registerBeans() {
		registerIfNotContains(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME,
				ListenerContainerFactoryResolver.class);
		registerIfNotContains(RetryTopicInternalBeanNames.DESTINATION_TOPIC_PROCESSOR_NAME,
				DefaultDestinationTopicProcessor.class);
		registerIfNotContains(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME,
				ListenerContainerFactoryConfigurer.class);
		registerIfNotContains(RetryTopicInternalBeanNames.DEAD_LETTER_PUBLISHING_RECOVERER_FACTORY_BEAN_NAME,
				DeadLetterPublishingRecovererFactory.class);
		registerIfNotContains(RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER, RetryTopicConfigurer.class);
		registerIfNotContains(RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME,
				DefaultDestinationTopicResolver.class);
		registerIfNotContains(RetryTopicInternalBeanNames.BACKOFF_SLEEPER_BEAN_NAME, ThreadWaitSleeper.class);
		registerIfNotContains(RetryTopicInternalBeanNames.INTERNAL_KAFKA_CONSUMER_BACKOFF_MANAGER_FACTORY,
				PartitionPausingBackOffManagerFactory.class);

		// Register a RetryTopicNamesProviderFactory implementation only if none is already present in the context
		try {
			this.applicationContext.getBean(RetryTopicNamesProviderFactory.class);
		}
		catch (NoSuchBeanDefinitionException e) {
			((BeanDefinitionRegistry) this.applicationContext).registerBeanDefinition(
					RetryTopicInternalBeanNames.RETRY_TOPIC_NAMES_PROVIDER_FACTORY,
					new RootBeanDefinition(SuffixingRetryTopicNamesProviderFactory.class));
		}
	}

	private void registerSingletons() {
		registerSingletonIfNotContains(RetryTopicInternalBeanNames.INTERNAL_BACKOFF_CLOCK_BEAN_NAME, Clock::systemUTC);
		registerSingletonIfNotContains(RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER,
				this::createKafkaConsumerBackoffManager);
	}

	private void addApplicationListeners() {
		ConfigurableApplicationContext context = (ConfigurableApplicationContext) this.applicationContext;
		context.addApplicationListener(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DefaultDestinationTopicResolver.class));
		context.addApplicationListener(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, PartitionPausingBackoffManager.class));
	}

	private KafkaConsumerBackoffManager createKafkaConsumerBackoffManager() {
		KafkaBackOffManagerFactory factory = this.applicationContext
				.getBean(RetryTopicInternalBeanNames.INTERNAL_KAFKA_CONSUMER_BACKOFF_MANAGER_FACTORY,
						KafkaBackOffManagerFactory.class);
		if (ApplicationContextAware.class.isAssignableFrom(factory.getClass())) {
			((ApplicationContextAware) factory).setApplicationContext(this.applicationContext);
		}
		if (PartitionPausingBackOffManagerFactory.class.isAssignableFrom(factory.getClass())) {
			setupTimingAdjustingBackOffFactory((PartitionPausingBackOffManagerFactory) factory);
		}
		return factory.create();
	}

	private void setupTimingAdjustingBackOffFactory(PartitionPausingBackOffManagerFactory factory) {
		if (this.applicationContext.containsBean(RetryTopicInternalBeanNames.BACKOFF_TASK_EXECUTOR)) {
			factory.setTaskExecutor(this.applicationContext
					.getBean(RetryTopicInternalBeanNames.BACKOFF_TASK_EXECUTOR, TaskExecutor.class));
		}
		if (this.applicationContext.containsBean(
				RetryTopicInternalBeanNames.INTERNAL_BACKOFF_TIMING_ADJUSTMENT_MANAGER)) {
			factory.setTimingAdjustmentManager(this.applicationContext
					.getBean(RetryTopicInternalBeanNames.INTERNAL_BACKOFF_TIMING_ADJUSTMENT_MANAGER,
						KafkaConsumerTimingAdjuster.class));
		}
	}

	private void registerIfNotContains(String beanName, Class<?> beanClass) {
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;
		if (!registry.containsBeanDefinition(beanName)) {
			registry.registerBeanDefinition(beanName,
					new RootBeanDefinition(beanClass));
		}
	}

	private void registerSingletonIfNotContains(String beanName, Supplier<Object> singletonSupplier) {
		if (!this.applicationContext.containsBeanDefinition(beanName)) {
			((SingletonBeanRegistry) this.beanFactory).registerSingleton(beanName, singletonSupplier.get());
		}
	}

}
