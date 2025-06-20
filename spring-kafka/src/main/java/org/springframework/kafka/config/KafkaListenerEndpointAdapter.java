/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.kafka.config;

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
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
	}

	@Override
	public @Nullable String getId() {
		return null;
	}

	@Override
	public @Nullable String getGroupId() {
		return null;
	}

	@Override
	public @Nullable String getGroup() {
		return null;
	}

	@Override
	public @Nullable Collection<String> getTopics() {
		return Collections.emptyList();
	}

	@Override
	public TopicPartitionOffset @Nullable [] getTopicPartitionsToAssign() {
		return new TopicPartitionOffset[0];
	}

	@Override
	public @Nullable Pattern getTopicPattern() {
		return null;
	}

	@Override
	public @Nullable String getClientIdPrefix() {
		return null;
	}

	@Override
	public @Nullable Integer getConcurrency() {
		return null;
	}

	@Override
	public @Nullable Boolean getAutoStartup() { // NOSONAR
		return null; // NOSONAR null check by caller
	}

	@Override
	public void setupListenerContainer(MessageListenerContainer listenerContainer,
			@Nullable MessageConverter messageConverter) {
	}

	@Override
	public boolean isSplitIterables() {
		return true;
	}

}
