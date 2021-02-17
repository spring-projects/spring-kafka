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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;

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
 *
 */
public class RetryTopicBootstrapper {

	private final ApplicationContext applicationContext;
	private final BeanFactory beanFactory;

	public RetryTopicBootstrapper(ApplicationContext applicationContext, BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (!ConfigurableApplicationContext.class.isAssignableFrom(applicationContext.getClass()) ||
			!BeanDefinitionRegistry.class.isAssignableFrom(applicationContext.getClass())) {
			throw new IllegalStateException(String.format("ApplicationContext must be implement %s and %s interfaces. Provided: %s",
					ConfigurableApplicationContext.class.getSimpleName(),
					BeanDefinitionRegistry.class.getSimpleName(),
					applicationContext.getClass().getSimpleName()));
		}
		if (!SingletonBeanRegistry.class.isAssignableFrom(this.beanFactory.getClass())) {
			throw new IllegalStateException("BeanFactory must implement " + SingletonBeanRegistry.class +
					" interface. Provided: " + this.beanFactory.getClass().getSimpleName());
		}
		this.applicationContext = applicationContext;
	}

	public void bootstrapRetryTopic() {
		registerBeans();
		configureBackoffClock();
		configureDestinationTopicContainer();
		configureKafkaConsumerBackoffManager();
	}

	private void registerBeans() {
		registerIfNotContains(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME,
				ListenerContainerFactoryResolver.class);
		registerIfNotContains(RetryTopicInternalBeanNames.DESTINATION_TOPIC_PROCESSOR_NAME,
				DefaultDestinationTopicProcessor.class);
		registerIfNotContains(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME,
				ListenerContainerFactoryConfigurer.class);
		registerIfNotContains(RetryTopicInternalBeanNames.DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME,
				DeadLetterPublishingRecovererFactory.class);
		registerIfNotContains(RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER, RetryTopicConfigurer.class);
		registerIfNotContains(RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class);
		registerIfNotContains(RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class);

	}

	private void configureBackoffClock() {
		if (!this.applicationContext.containsBeanDefinition(KafkaConsumerBackoffManager.INTERNAL_BACKOFF_CLOCK_BEAN_NAME)) {
			((SingletonBeanRegistry) this.beanFactory).registerSingleton(
					KafkaConsumerBackoffManager.INTERNAL_BACKOFF_CLOCK_BEAN_NAME, Clock.systemUTC());
		}
	}

	private void configureKafkaConsumerBackoffManager() {
		KafkaConsumerBackoffManager kafkaConsumerBackoffManager = this.applicationContext.getBean(
				RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class);
		((ConfigurableApplicationContext) this.applicationContext).addApplicationListener(kafkaConsumerBackoffManager);
	}

	private void configureDestinationTopicContainer() {
		DestinationTopicContainer destinationTopicContainer = this.applicationContext.getBean(
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class);
		((ConfigurableApplicationContext) this.applicationContext).addApplicationListener(destinationTopicContainer);
	}

	private void registerIfNotContains(String beanName, Class<?> beanClass) {
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;
		if (!registry.containsBeanDefinition(beanName)) {
			registry.registerBeanDefinition(beanName,
					new RootBeanDefinition(beanClass));
		}
	}
}
