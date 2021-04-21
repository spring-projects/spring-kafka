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

import org.springframework.kafka.config.MethodKafkaListenerEndpoint;

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
		String getEndpointId(MethodKafkaListenerEndpoint<?, ?> endpoint);

		/**
		 * Return the groupId that will override the endpoint's groupId.
		 *
		 * @param endpoint the endpoint to override
		 * @return The groupId
		 */
		String getGroupId(MethodKafkaListenerEndpoint<?, ?> endpoint);

		/**
		 * Return the clientId prefix that will override the endpoint's clientId prefix.
		 *
		 * @param endpoint the endpoint to override
		 * @return The clientId prefix
		 */
		String getClientIdPrefix(MethodKafkaListenerEndpoint<?, ?> endpoint);

		/**
		 * Return the group that will override the endpoint's group.
		 *
		 * @param endpoint the endpoint to override
		 * @return The clientId prefix
		 */
		String getGroup(MethodKafkaListenerEndpoint<?, ?> endpoint);

		/**
		 * Return the tropic name that will override the base topic name.
		 *
		 * @param topic the base topic name
		 * @return The topic name
		 */
		String getTopicName(String topic);

	}

}
