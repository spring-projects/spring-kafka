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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
class RetryTopicInternalBeanNamesTests {

	static final String DESTINATION_TOPIC_PROCESSOR_NAME = "internalDestinationTopicProcessor";

	static final String KAFKA_CONSUMER_BACKOFF_MANAGER = "internalKafkaConsumerBackoffManager";

	static final String RETRY_TOPIC_CONFIGURER = "internalRetryTopicConfigurer";

	static final String LISTENER_CONTAINER_FACTORY_RESOLVER_NAME = "internalListenerContainerFactoryResolver";

	static final String LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME = "internalListenerContainerFactoryConfigurer";

	static final String DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME = "internalDeadLetterPublishingRecovererProvider";

	static final String DESTINATION_TOPIC_CONTAINER_NAME = "internalDestinationTopicContainer";

	static final String DEFAULT_LISTENER_FACTORY_BEAN_NAME = "internalRetryTopicListenerContainerFactory";

	static final String DEFAULT_KAFKA_TEMPLATE_BEAN_NAME = "retryTopicDefaultKafkaTemplate";

	@Test
	public void assertRetryTopicInternalBeanNamesConstants() {
		new RetryTopicInternalBeanNames() { }; // for coverage
		assertThat(RetryTopicInternalBeanNames.DESTINATION_TOPIC_PROCESSOR_NAME)
				.isEqualTo(DESTINATION_TOPIC_PROCESSOR_NAME);
		assertThat(RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER)
				.isEqualTo(KAFKA_CONSUMER_BACKOFF_MANAGER);
		assertThat(RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER).isEqualTo(RETRY_TOPIC_CONFIGURER);
		assertThat(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME)
				.isEqualTo(LISTENER_CONTAINER_FACTORY_RESOLVER_NAME);
		assertThat(RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME)
				.isEqualTo(LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME);
		assertThat(RetryTopicInternalBeanNames.DEAD_LETTER_PUBLISHING_RECOVERER_FACTORY_BEAN_NAME)
				.isEqualTo(DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME);
		assertThat(RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME)
				.isEqualTo(DESTINATION_TOPIC_CONTAINER_NAME);
		assertThat(RetryTopicInternalBeanNames.DEFAULT_LISTENER_FACTORY_BEAN_NAME)
				.isEqualTo(DEFAULT_LISTENER_FACTORY_BEAN_NAME);
		assertThat(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME)
				.isEqualTo(DEFAULT_KAFKA_TEMPLATE_BEAN_NAME);
	}
}
