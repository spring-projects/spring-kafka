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

import java.time.Clock;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.ContainerPartitionPausingBackOffManagerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaBackOffManagerFactory;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;

/**
 * Provide the component instances that will be used with
 * {@link RetryTopicConfigurationSupport}. Override any of the methods to provide
 * a different implementation or subclass, then override the
 * {@link RetryTopicConfigurationSupport#createComponentFactory()} method
 * to return this factory's subclass.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public class RetryTopicComponentFactory {

	private final Clock internalRetryTopicClock = createInternalRetryTopicClock();

	/**
	 * Create the {@link RetryTopicConfigurer} that will serve as an entry point
	 * for configuring non-blocking topic-based delayed retries for a given
	 * {@link KafkaListenerEndpoint}, by processing the appropriate
	 * {@link RetryTopicConfiguration}.
	 * @param destinationTopicProcessor the {@link DestinationTopicProcessor} that will be
	 * used to process the {@link DestinationTopic} instances and register them in a
	 * {@link DestinationTopicContainer}.
	 * @param listenerContainerFactoryConfigurer the
	 * {@link ListenerContainerFactoryConfigurer} that will be used to configure the
	 * {@link KafkaListenerContainerFactory} instances for the non-blocking delayed
	 * retries feature.
	 * @param factoryResolver the {@link ListenerContainerFactoryResolver} that will
	 * be used to resolve the proper {@link KafkaListenerContainerFactory} for a given
	 * endpoint or its retry topics.
	 * @param retryTopicNamesProviderFactory the {@link RetryTopicNamesProviderFactory}
	 * that will be used to provide the property names for the retry topics' endpoints.
	 * @return the instance.
	 */
	public RetryTopicConfigurer retryTopicConfigurer(DestinationTopicProcessor destinationTopicProcessor,
			ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer,
			ListenerContainerFactoryResolver factoryResolver,
			RetryTopicNamesProviderFactory retryTopicNamesProviderFactory) {

		return new RetryTopicConfigurer(destinationTopicProcessor, factoryResolver,
				listenerContainerFactoryConfigurer, retryTopicNamesProviderFactory);
	}

	/**
	 * Create the {@link DestinationTopicProcessor} that will be used to process the
	 * {@link DestinationTopic} instances and store them in the provided
	 * {@link DestinationTopicResolver}.
	 * @param destinationTopicResolver the {@link DestinationTopicResolver}
	 * instance to be used to store the {@link DestinationTopic} instances.
	 * @return the instance.
	 */
	public DestinationTopicProcessor destinationTopicProcessor(DestinationTopicResolver destinationTopicResolver) {
		return new DefaultDestinationTopicProcessor(destinationTopicResolver);
	}

	/**
	 * Create the instance of {@link DestinationTopicResolver} that will be used to store
	 * the {@link DestinationTopic} instance and resolve which a given record should be
	 * forwarded to.
	 * @return the instance.
	 */
	public DestinationTopicResolver destinationTopicResolver() {
		return new DefaultDestinationTopicResolver(this.internalRetryTopicClock);
	}

	/**
	 * Create the {@link DeadLetterPublishingRecovererFactory} that will be used to create
	 * the {@link DeadLetterPublishingRecoverer} to forward the records to a given
	 * {@link DestinationTopic}.
	 * @param destinationTopicResolver the {@link DestinationTopicResolver} instance to
	 * resolve the destinations.
	 * @return the instance.
	 */
	public DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory(
			DestinationTopicResolver destinationTopicResolver) {

		return new DeadLetterPublishingRecovererFactory(destinationTopicResolver);
	}

	/**
	 * Create the {@link ListenerContainerFactoryResolver} that will be used to resolve
	 * the appropriate {@link KafkaListenerContainerFactory} for a given topic.
	 * @param beanFactory the {@link BeanFactory} that will be used to retrieve the
	 * {@link KafkaListenerContainerFactory} instance if necessary.
	 * @return the instance.
	 */
	public ListenerContainerFactoryResolver listenerContainerFactoryResolver(BeanFactory beanFactory) {
		return new ListenerContainerFactoryResolver(beanFactory);
	}

	/**
	 * Create a {@link ListenerContainerFactoryConfigurer} that will be used to
	 * configure the {@link KafkaListenerContainerFactory} resolved by the
	 * {@link ListenerContainerFactoryResolver}.
	 * @param kafkaConsumerBackoffManager the {@link KafkaConsumerBackoffManager} used
	 * with the {@link KafkaBackoffAwareMessageListenerAdapter}.
	 * @param deadLetterPublishingRecovererFactory the factory that will provide the
	 * {@link DeadLetterPublishingRecoverer} instance to be used.
	 * @param clock the {@link Clock} instance to be used with the listener adapter.
	 * @return the instance.
	 */
	public ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer(KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
			DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory,
			Clock clock) {

		return new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory, clock);
	}

	/**
	 * Create the {@link RetryTopicNamesProviderFactory} instance that will be used
	 * to provide the property names for the retry topics' {@link KafkaListenerEndpoint}.
	 * @return the instance.
	 */
	public RetryTopicNamesProviderFactory retryTopicNamesProviderFactory() {
		return new SuffixingRetryTopicNamesProviderFactory();
	}

	/**
	 * Create the {@link KafkaBackOffManagerFactory} that will be used to create the
	 * {@link KafkaConsumerBackoffManager} instance used to back off the partitions.
	 * @param registry the {@link ListenerContainerRegistry} used to fetch the
	 * {@link MessageListenerContainer}.
	 * @param applicationContext the application context.
	 * @return the instance.
	 */
	public KafkaBackOffManagerFactory kafkaBackOffManagerFactory(ListenerContainerRegistry registry,
			ApplicationContext applicationContext) {

		return new ContainerPartitionPausingBackOffManagerFactory(registry, applicationContext);
	}

	/**
	 * Return the {@link Clock} instance that will be used for all
	 * time-related operations in the retry topic processes.
	 * @return the instance.
	 */
	public Clock internalRetryTopicClock() {
		return this.internalRetryTopicClock;
	}

	/**
	 * Create a {@link Clock} instance that will be used for all time-related operations
	 * in the retry topic processes.
	 * @return the instance.
	 */
	protected Clock createInternalRetryTopicClock() {
		return Clock.systemUTC();
	}

}
