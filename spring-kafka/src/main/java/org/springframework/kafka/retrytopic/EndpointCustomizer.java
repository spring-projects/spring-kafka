/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.config.MethodKafkaListenerEndpoint;

/**
 * Customizes main, retry and DLT endpoints in the Retry Topic functionality
 * and returns the resulting topic names.
 *
 * @param <T> the listener endpoint type.
 *
 * @author Tomaz Fernandes
 * @author Wang Zhiyang
 *
 * @since 2.7.2
 *
 * @see EndpointCustomizerFactory
 *
 */
@FunctionalInterface
public interface EndpointCustomizer<T extends MethodKafkaListenerEndpoint<?, ?>> {

	/**
	 * Customize the endpoint and return the topic names generated for this endpoint.
	 * @param listenerEndpoint The main, retry or DLT endpoint to be customized.
	 * @return A collection containing the topic names generated for this endpoint.
	 */
	Collection<TopicNamesHolder> customizeEndpointAndCollectTopics(T listenerEndpoint);

	record TopicNamesHolder(String mainTopic, @Nullable String customizedTopic) {
	}
}
