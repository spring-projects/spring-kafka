/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.support.Suffixer;

/**
 * Retry and dead letter naming handling that add a suffix to each name.
 * The suffix is taken from {@link DestinationTopic.Properties#suffix()}
 *
 * @author Andrea Polci
 */
public class SuffixingRetryTopicNamesProviderFactory implements RetryTopicNamesProviderFactory {

	@Override
	public RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties) {
		return new SuffixingRetryTopicNamesProvider(properties);
	}

	public static class SuffixingRetryTopicNamesProvider implements RetryTopicNamesProvider {

		private final Suffixer suffixer;

		public SuffixingRetryTopicNamesProvider(DestinationTopic.Properties properties) {
			this.suffixer = new Suffixer(properties.suffix());
		}

		@Override
		public @Nullable String getEndpointId(KafkaListenerEndpoint endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getId());
		}

		@Override
		public @Nullable String getGroupId(KafkaListenerEndpoint endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getGroupId());
		}

		@Override
		public @Nullable String getClientIdPrefix(KafkaListenerEndpoint endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getClientIdPrefix());
		}

		@Override
		public @Nullable String getGroup(KafkaListenerEndpoint endpoint) {
			return this.suffixer.maybeAddTo(endpoint.getGroup());
		}

		@Override
		public @Nullable String getTopicName(String topic) {
			return this.suffixer.maybeAddTo(topic);
		}

	}

}
