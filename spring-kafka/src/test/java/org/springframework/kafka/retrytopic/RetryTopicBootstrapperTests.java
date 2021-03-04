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

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import java.time.Clock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class RetryTopicBootstrapperTests {

	@Mock
	private ApplicationContext wrongApplicationContext;

	@Mock
	private GenericApplicationContext applicationContext;

	@Mock
	private DefaultListableBeanFactory beanFactory;

	@Mock
	private BeanFactory wrongBeanFactory;

	@Mock
	private DestinationTopicContainer destinationTopicContainer;

	@Mock
	private KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	@Test
	void shouldThrowIfACDoesntImplementInterfaces() {
		assertThatIllegalStateException()
				.isThrownBy(() -> new RetryTopicBootstrapper(wrongApplicationContext, beanFactory));
	}

	@Test
	void shouldThrowIfBFDoesntImplementInterfaces() {
		assertThatIllegalStateException()
				.isThrownBy(() -> new RetryTopicBootstrapper(applicationContext, wrongBeanFactory));
	}

	@Test
	void shouldRegisterBeansIfNotRegistered() {

		// given
		given(applicationContext.containsBeanDefinition(any(String.class))).willReturn(false);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class))
				.willReturn(destinationTopicContainer);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class))
				.willReturn(kafkaConsumerBackoffManager);

		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;

		// when
		RetryTopicBootstrapper bootstrapper = new RetryTopicBootstrapper(applicationContext, beanFactory);
		bootstrapper.bootstrapRetryTopic();

		// then
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME, new RootBeanDefinition(ListenerContainerFactoryResolver.class));
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.DESTINATION_TOPIC_PROCESSOR_NAME, new RootBeanDefinition(DefaultDestinationTopicProcessor.class));
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME, new RootBeanDefinition(ListenerContainerFactoryConfigurer.class));
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME, new RootBeanDefinition(DeadLetterPublishingRecovererFactory.class));
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER, new RootBeanDefinition(RetryTopicConfigurer.class));
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, new RootBeanDefinition(KafkaConsumerBackoffManager.class));
		then(registry).should(times(1)).registerBeanDefinition(RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, new RootBeanDefinition(DestinationTopicContainer.class));
	}

	@Test
	void shouldNotRegisterBeansIfRegistered() {

		// given
		given(applicationContext.containsBeanDefinition(any(String.class))).willReturn(true);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class))
				.willReturn(destinationTopicContainer);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class))
				.willReturn(kafkaConsumerBackoffManager);

		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;

		// when
		RetryTopicBootstrapper bootstrapper = new RetryTopicBootstrapper(applicationContext, beanFactory);
		bootstrapper.bootstrapRetryTopic();

		// then
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME, new RootBeanDefinition(ListenerContainerFactoryResolver.class));
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.DESTINATION_TOPIC_PROCESSOR_NAME, new RootBeanDefinition(DefaultDestinationTopicProcessor.class));
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME, new RootBeanDefinition(ListenerContainerFactoryConfigurer.class));
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME, new RootBeanDefinition(DeadLetterPublishingRecovererFactory.class));
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER, new RootBeanDefinition(RetryTopicConfigurer.class));
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, new RootBeanDefinition(KafkaConsumerBackoffManager.class));
		then(registry).should(times(0)).registerBeanDefinition(RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, new RootBeanDefinition(DestinationTopicContainer.class));
	}

	@Test
	void shouldConfigureClock() {

		// given
		given(applicationContext.containsBeanDefinition(any(String.class)))
				.willReturn(false);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class))
				.willReturn(destinationTopicContainer);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class))
				.willReturn(kafkaConsumerBackoffManager);

		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;

		// when
		RetryTopicBootstrapper bootstrapper = new RetryTopicBootstrapper(applicationContext, beanFactory);
		bootstrapper.bootstrapRetryTopic();

		// then
		then((SingletonBeanRegistry) this.beanFactory).should(times(1)).registerSingleton(
				KafkaConsumerBackoffManager.INTERNAL_BACKOFF_CLOCK_BEAN_NAME, Clock.systemUTC());
	}

	@Test
	void shouldAddApplicationListeners() {

		// given
		given(applicationContext.containsBeanDefinition(any(String.class)))
				.willReturn(false);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class))
				.willReturn(destinationTopicContainer);
		given(this.applicationContext.getBean(
				RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class))
				.willReturn(kafkaConsumerBackoffManager);

		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;

		// when
		RetryTopicBootstrapper bootstrapper = new RetryTopicBootstrapper(applicationContext, beanFactory);
		bootstrapper.bootstrapRetryTopic();
		ConfigurableApplicationContext configurableApplicationContext = (ConfigurableApplicationContext) this.applicationContext;

		// then
		then(configurableApplicationContext).should(times(1)).addApplicationListener(kafkaConsumerBackoffManager);
		then(configurableApplicationContext).should(times(1)).addApplicationListener(destinationTopicContainer);
	}
}
