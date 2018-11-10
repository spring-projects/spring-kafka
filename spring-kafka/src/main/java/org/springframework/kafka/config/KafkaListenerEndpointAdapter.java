/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.config;

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.support.converter.MessageConverter;

/**
 * Adapter to avoid having to implement all methods.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
class KafkaListenerEndpointAdapter implements KafkaListenerEndpoint {

	KafkaListenerEndpointAdapter() {
		super();
	}

	@Override
	public String getId() {
		return null;
	}

	@Override
	public String getGroupId() {
		return null;
	}

	@Override
	public String getGroup() {
		return null;
	}

	@Override
	public Collection<String> getTopics() {
		return Collections.emptyList();
	}

	@Override
	public Collection<TopicPartitionInitialOffset> getTopicPartitions() {
		return Collections.emptyList();
	}

	@Override
	public Pattern getTopicPattern() {
		return null;
	}

	@Override
	public String getClientIdPrefix() {
		return null;
	}

	@Override
	public Integer getConcurrency() {
		return null;
	}

	@Override
	public Boolean getAutoStartup() { // NOSONAR
		return null; // NOSONAR null check by caller
	}

	@Override
	public void setupListenerContainer(MessageListenerContainer listenerContainer,
			MessageConverter messageConverter) {
	}

}
