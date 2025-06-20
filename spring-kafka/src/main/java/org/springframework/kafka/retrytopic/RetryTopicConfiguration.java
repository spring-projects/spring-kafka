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

package org.springframework.kafka.retrytopic;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.AllowDenyCollectionManager;
import org.springframework.kafka.support.EndpointHandlerMethod;

/**
 * Contains the provided configuration for the retryable topics.
 *
 * Should be created via the {@link RetryTopicConfigurationBuilder}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @since 2.7
 *
 */
public class RetryTopicConfiguration {

	private final List<DestinationTopic.Properties> destinationTopicProperties;

	private final AllowDenyCollectionManager<String> topicAllowListManager;

	private final @Nullable EndpointHandlerMethod dltHandlerMethod;

	private final TopicCreation kafkaTopicAutoCreationConfig;

	private final ListenerContainerFactoryResolver.Configuration factoryResolverConfig;

	@Nullable
	private final Integer concurrency;

	RetryTopicConfiguration(List<DestinationTopic.Properties> destinationTopicProperties,
							@Nullable EndpointHandlerMethod dltHandlerMethod,
							TopicCreation kafkaTopicAutoCreationConfig,
							AllowDenyCollectionManager<String> topicAllowListManager,
							ListenerContainerFactoryResolver.Configuration factoryResolverConfig,
							@Nullable Integer concurrency) {
		this.destinationTopicProperties = destinationTopicProperties;
		this.dltHandlerMethod = dltHandlerMethod;
		this.kafkaTopicAutoCreationConfig = kafkaTopicAutoCreationConfig;
		this.topicAllowListManager = topicAllowListManager;
		this.factoryResolverConfig = factoryResolverConfig;
		this.concurrency = concurrency;
	}

	public boolean hasConfigurationForTopics(String[] topics) {
		return this.topicAllowListManager.areAllowed(topics);
	}

	public TopicCreation forKafkaTopicAutoCreation() {
		return this.kafkaTopicAutoCreationConfig;
	}

	public ListenerContainerFactoryResolver.Configuration forContainerFactoryResolver() {
		return this.factoryResolverConfig;
	}

	public @Nullable EndpointHandlerMethod getDltHandlerMethod() {
		return this.dltHandlerMethod;
	}

	public List<DestinationTopic.Properties> getDestinationTopicProperties() {
		return this.destinationTopicProperties;
	}

	@Nullable
	public Integer getConcurrency() {
		return this.concurrency;
	}

	static class TopicCreation {

		private final boolean shouldCreateTopics;

		private final int numPartitions;

		private final short replicationFactor;

		TopicCreation(@Nullable Boolean shouldCreate, @Nullable Integer numPartitions, @Nullable Short replicationFactor) {
			this.shouldCreateTopics = shouldCreate == null || shouldCreate;
			this.numPartitions = numPartitions == null ? 1 : numPartitions;
			this.replicationFactor = replicationFactor == null ? -1 : replicationFactor;
		}

		TopicCreation() {
			this.shouldCreateTopics = true;
			this.numPartitions = 1;
			this.replicationFactor = -1;
		}

		TopicCreation(boolean shouldCreateTopics) {
			this.shouldCreateTopics = shouldCreateTopics;
			this.numPartitions = 1;
			this.replicationFactor = -1;
		}

		public int getNumPartitions() {
			return this.numPartitions;
		}

		public short getReplicationFactor() {
			return this.replicationFactor;
		}

		public boolean shouldCreateTopics() {
			return this.shouldCreateTopics;
		}
	}

}
