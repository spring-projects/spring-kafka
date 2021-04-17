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
 * @see DefaultRetryTopicNamesProviderFactory
 */
public interface RetryTopicNamesProviderFactory {

	RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties);

	interface RetryTopicNamesProvider {
		String getEndpointId(MethodKafkaListenerEndpoint<?, ?> endpoint);

		String getGroupId(MethodKafkaListenerEndpoint<?, ?> endpoint);

		String getClientIdPrefix(MethodKafkaListenerEndpoint<?, ?> endpoint);

		String getGroup(MethodKafkaListenerEndpoint<?, ?> endpoint);

		String getTopicName(String topic);
	}
}
