/*
 * Copyright 2021-present the original author or authors.
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
