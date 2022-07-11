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

/**
 * The bean names for the non-blocking topic-based delayed retries feature.
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.9
 */
public final class RetryTopicBeanNames {

	private RetryTopicBeanNames() {
	}

	/**
	 * The bean name of an internally managed retry topic configuration support, if
	 * needed.
	 */
	public static final String DEFAULT_RETRY_TOPIC_CONFIG_SUPPORT_BEAN_NAME =
			"org.springframework.kafka.retrytopic.internalRetryTopicConfigurationSupport";

	/**
	 * The bean name of the internally managed retry topic configurer.
	 */
	public static final String RETRY_TOPIC_CONFIGURER_BEAN_NAME =
			"org.springframework.kafka.retrytopic.internalRetryTopicConfigurer";

	/**
	 * The bean name of the internally managed destination topic resolver.
	 */
	public static final String DESTINATION_TOPIC_RESOLVER_BEAN_NAME =
			"org.springframework.kafka.retrytopic.internalDestinationTopicResolver";

	/**
	 * The bean name of the internally managed listener container factory.
	 */
	public static final String DEFAULT_LISTENER_CONTAINER_FACTORY_BEAN_NAME =
			"defaultRetryTopicListenerContainerFactory";

	/**
	 * The bean name of the internally managed listener container factory.
	 */
	public static final String DEFAULT_KAFKA_TEMPLATE_BEAN_NAME =
			"defaultRetryTopicKafkaTemplate";

	/**
	 * The bean name of the internally registered scheduler wrapper, if needed.
	 */
	public static final String DEFAULT_SCHEDULER_WRAPPER_BEAN_NAME =
			"defaultRetryTopicKafkaTemplate";

}
