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

/**
 *
 * Contains the internal bean names that will be used by the retryable topic configuration.
 *
 * If you provide a bean of your own with the same name that instance will be used instead
 * of the default one.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public abstract class RetryTopicInternalBeanNames {

	static final String DESTINATION_TOPIC_PROCESSOR_NAME = "internalDestinationTopicProcessor";

	static final String KAFKA_CONSUMER_BACKOFF_MANAGER = "internalKafkaConsumerBackoffManager";

	static final String RETRY_TOPIC_CONFIGURER = "internalRetryTopicConfigurer";

	static final String LISTENER_CONTAINER_FACTORY_RESOLVER_NAME = "internalListenerContainerFactoryResolver";

	static final String LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME = "internalListenerContainerFactoryConfigurer";

	static final String DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME = "internalDeadLetterPublishingRecovererProvider";

	static final String DESTINATION_TOPIC_CONTAINER_NAME = "internalDestinationTopicContainer";

	static final String DEFAULT_LISTENER_FACTORY_BEAN_NAME = "internalRetryTopicListenerContainerFactory";

	static final String BACKOFF_SLEEPER_BEAN_NAME = "internalBackoffSleeper";

	static final String BACKOFF_TASK_EXECUTOR = "internalBackOffTaskExecutor";

	static final String INTERNAL_BACKOFF_TIMING_ADJUSTMENT_MANAGER = "internalKafkaConsumerTimingAdjustmentManager";

	static final String INTERNAL_KAFKA_CONSUMER_BACKOFF_MANAGER_FACTORY = "internalKafkaConsumerBackOffManagerFactory";

	static final String RETRY_TOPIC_NAMES_PROVIDER_FACTORY = "internalRetryTopicNamesProviderFactory";

	/**
	 * Internal Back Off Clock Bean Name.
	 */
	public static final String INTERNAL_BACKOFF_CLOCK_BEAN_NAME = "internalBackOffClock";

	/**
	 * Default Kafka template bean name for publishing to retry topics.
	 */
	public static final String DEFAULT_KAFKA_TEMPLATE_BEAN_NAME = "retryTopicDefaultKafkaTemplate";

}
