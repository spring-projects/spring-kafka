/*
 * Copyright 2022-present the original author or authors.
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
			"defaultRetryTopicSchedulerWrapper";

}
