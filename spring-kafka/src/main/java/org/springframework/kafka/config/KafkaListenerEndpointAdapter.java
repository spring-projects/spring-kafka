/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
