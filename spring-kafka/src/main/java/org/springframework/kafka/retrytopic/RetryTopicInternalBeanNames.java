/*
 * Copyright 2021 the original author or authors.
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

/**
 *
 * Contains the internal bean names that will be used by the retryable topic configuration.
 *
 * If you provide a bean of your own with the same name, your instance will be used instead
 * of the default one.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public abstract class RetryTopicInternalBeanNames {

	/**
	 * {@link DestinationTopicProcessor} bean name.
	 */
	public static final String DESTINATION_TOPIC_PROCESSOR_NAME = "internalDestinationTopicProcessor";

	/**
	 *  {@link org.springframework.kafka.listener.KafkaConsumerBackoffManager} bean name.
	 */
	public static final String KAFKA_CONSUMER_BACKOFF_MANAGER = "internalKafkaConsumerBackoffManager";

	/**
	 * {@link ListenerContainerFactoryResolver} bean name.
	 */
	public static final String LISTENER_CONTAINER_FACTORY_RESOLVER_NAME = "internalListenerContainerFactoryResolver";

	/**
	 * {@link ListenerContainerFactoryConfigurer} bean name.
	 */
	public static final String LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME = "internalListenerContainerFactoryConfigurer";

	/**
	 * {@link DeadLetterPublishingRecovererFactory} bean name.
	 */
	public static final String DEAD_LETTER_PUBLISHING_RECOVERER_FACTORY_BEAN_NAME =
			"internalDeadLetterPublishingRecovererProvider";

	/**
	 * {@link DeadLetterPublishingRecovererFactory} bean name.
	 * @deprecated in favor of {@link #DEAD_LETTER_PUBLISHING_RECOVERER_FACTORY_BEAN_NAME}
	 */
	@Deprecated
	public static final String DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME =
			DEAD_LETTER_PUBLISHING_RECOVERER_FACTORY_BEAN_NAME;

	/**
	 * {@link DestinationTopicContainer} bean name.
	 */
	public static final String DESTINATION_TOPIC_CONTAINER_NAME = "internalDestinationTopicContainer";

	/**
	 * The default {@link org.springframework.kafka.config.KafkaListenerContainerFactory}
	 * bean name that will be looked up if no other is provided.
	 */
	public static final String DEFAULT_LISTENER_FACTORY_BEAN_NAME = "internalRetryTopicListenerContainerFactory";

	/**
	 * {@link org.springframework.retry.backoff.Sleeper} bean name.
	 */
	public static final String BACKOFF_SLEEPER_BEAN_NAME = "internalBackoffSleeper";

	/**
	 * {@link org.springframework.core.task.TaskExecutor} bean name to be used.
	 * in the {@link org.springframework.kafka.listener.WakingKafkaConsumerTimingAdjuster}
	 */
	public static final String BACKOFF_TASK_EXECUTOR = "internalBackOffTaskExecutor";

	/**
	 * {@link org.springframework.kafka.listener.KafkaConsumerTimingAdjuster} bean name.
	 */
	public static final String INTERNAL_BACKOFF_TIMING_ADJUSTMENT_MANAGER = "internalKafkaConsumerTimingAdjustmentManager";

	/**
	 * {@link org.springframework.kafka.listener.KafkaBackOffManagerFactory} bean name.
	 */
	public static final String INTERNAL_KAFKA_CONSUMER_BACKOFF_MANAGER_FACTORY = "internalKafkaConsumerBackOffManagerFactory";

	/**
	 * {@link RetryTopicNamesProviderFactory} bean name.
	 */
	public static final String RETRY_TOPIC_NAMES_PROVIDER_FACTORY = "internalRetryTopicNamesProviderFactory";

	/**
	 * The {@link java.time.Clock} bean name that will be used for backing off partitions.
	 */
	public static final String INTERNAL_BACKOFF_CLOCK_BEAN_NAME = "internalBackOffClock";

	/**
	 * Default {@link org.springframework.kafka.core.KafkaTemplate} bean name for publishing to retry topics.
	 */
	public static final String DEFAULT_KAFKA_TEMPLATE_BEAN_NAME = "retryTopicDefaultKafkaTemplate";

	/**
	 * {@link RetryTopicBootstrapper} bean name.
	 */
	public static final String RETRY_TOPIC_BOOTSTRAPPER = "internalRetryTopicBootstrapper";

	/**
	 * {@link RetryTopicConfigurer} bean name.
	 */
	public static final String RETRY_TOPIC_CONFIGURER = "internalRetryTopicConfigurer";

}
