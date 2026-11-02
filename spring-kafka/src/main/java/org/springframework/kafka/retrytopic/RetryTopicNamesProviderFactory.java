/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.config.KafkaListenerEndpoint;

/**
 * Handles the naming related to the retry and dead letter topics.
 *
 * @author Andrea Polci
 * @see SuffixingRetryTopicNamesProviderFactory
 */
public interface RetryTopicNamesProviderFactory {

	RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties);

	interface RetryTopicNamesProvider {

		/**
		 * Return the endpoint id that will override the endpoint's current id.
		 *
		 * @param endpoint the endpoint to override
		 * @return The endpoint id
		 */
		@Nullable
		String getEndpointId(KafkaListenerEndpoint endpoint);

		/**
		 * Return the groupId that will override the endpoint's groupId.
		 *
		 * @param endpoint the endpoint to override
		 * @return The groupId
		 */
		@Nullable
		String getGroupId(KafkaListenerEndpoint endpoint);

		/**
		 * Return the clientId prefix that will override the endpoint's clientId prefix.
		 *
		 * @param endpoint the endpoint to override
		 * @return The clientId prefix
		 */
		@Nullable
		String getClientIdPrefix(KafkaListenerEndpoint endpoint);

		/**
		 * Return the group that will override the endpoint's group.
		 *
		 * @param endpoint the endpoint to override
		 * @return The clientId prefix
		 */
		@Nullable
		String getGroup(KafkaListenerEndpoint endpoint);

		/**
		 * Return the tropic name that will override the base topic name.
		 *
		 * @param topic the base topic name
		 * @return The topic name
		 */
		@Nullable
		String getTopicName(String topic);

	}

}
