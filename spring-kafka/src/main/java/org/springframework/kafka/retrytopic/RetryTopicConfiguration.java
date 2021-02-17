/*
 * Copyright 2018-2021 the original author or authors.
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

import org.springframework.kafka.support.AllowDenyCollectionManager;

/**
 *
 * Contains the provided configuration for the retryable topics.
 *
 * Should be created via the {@link RetryTopicConfigurationBuilder}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class RetryTopicConfiguration {

	private final List<DestinationTopic.Properties> destinationTopicProperties;

	private final AllowDenyCollectionManager<String> topicAllowListManager;

	private final DeadLetterPublishingRecovererFactory.Configuration deadLetterProviderConfiguration;

	private final RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod;

	private final TopicCreation kafkaTopicAutoCreation;

	private final ListenerContainerFactoryResolver.Configuration factoryResolverConfig;

	RetryTopicConfiguration(List<DestinationTopic.Properties> destinationTopicProperties,
								DeadLetterPublishingRecovererFactory.Configuration deadLetterProviderConfiguration,
								RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod,
								TopicCreation kafkaTopicAutoCreation,
								AllowDenyCollectionManager<String> topicAllowListManager,
								ListenerContainerFactoryResolver.Configuration factoryResolverConfig) {
		this.destinationTopicProperties = destinationTopicProperties;
		this.deadLetterProviderConfiguration = deadLetterProviderConfiguration;
		this.dltHandlerMethod = dltHandlerMethod;
		this.kafkaTopicAutoCreation = kafkaTopicAutoCreation;
		this.topicAllowListManager = topicAllowListManager;
		this.factoryResolverConfig = factoryResolverConfig;
	}

	public boolean hasConfigurationForTopics(String[] topics) {
		return this.topicAllowListManager.areAllowed(topics);
	}

	public TopicCreation forKafkaTopicAutoCreation() {
		return this.kafkaTopicAutoCreation;
	}

	public DeadLetterPublishingRecovererFactory.Configuration forDeadLetterFactory() {
		return this.deadLetterProviderConfiguration;
	}

	public ListenerContainerFactoryResolver.Configuration forContainerFactoryResolver() {
		return this.factoryResolverConfig;
	}

	public RetryTopicConfigurer.EndpointHandlerMethod getDltHandlerMethod() {
		return this.dltHandlerMethod;
	}

	public List<DestinationTopic.Properties> getDestinationTopicProperties() {
		return this.destinationTopicProperties;
	}

	public static RetryTopicConfigurationBuilder builder() {
		return new RetryTopicConfigurationBuilder();
	}

	static class TopicCreation {

		private final boolean shouldCreateTopics;
		private final int numPartitions;
		private final short replicationFactor;

		TopicCreation(boolean shouldCreate, int numPartitions, short replicationFactor) {
			this.shouldCreateTopics = shouldCreate;
			this.numPartitions = numPartitions;
			this.replicationFactor = replicationFactor;
		}

		TopicCreation() {
			this.shouldCreateTopics = true;
			this.numPartitions = 1;
			this.replicationFactor = 1;
		}

		TopicCreation(boolean shouldCreateTopics) {
			this.shouldCreateTopics = shouldCreateTopics;
			this.numPartitions = 1;
			this.replicationFactor = 1;
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
