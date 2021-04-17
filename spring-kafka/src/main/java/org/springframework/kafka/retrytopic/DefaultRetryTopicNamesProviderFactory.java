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
import org.springframework.kafka.support.Suffixer;

/**
 * Default handling of retry and dead letter naming.
 *
 * @author Andrea Polci
 */
public class DefaultRetryTopicNamesProviderFactory implements RetryTopicNamesProviderFactory {
	@Override
	public RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties) {
		return new DefaultRetryTopicNamesProvider(properties);
	}

	public static class DefaultRetryTopicNamesProvider implements RetryTopicNamesProvider {
		private final Suffixer suffixer;

		public DefaultRetryTopicNamesProvider(DestinationTopic.Properties properties) {
			this.suffixer = new Suffixer(properties.suffix());
		}

		@Override
		public String getEndpointId(MethodKafkaListenerEndpoint<?, ?> endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getId());
		}

		@Override
		public String getGroupId(MethodKafkaListenerEndpoint<?, ?> endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getGroupId());
		}

		@Override
		public String getClientIdPrefix(MethodKafkaListenerEndpoint<?, ?> endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getClientIdPrefix());
		}

		@Override
		public String getGroup(MethodKafkaListenerEndpoint<?, ?> endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getGroup());
		}

		@Override
		public String getTopicName(String topic) {
			return this.suffixer.maybeAddTo(topic);
		}
	}
}
