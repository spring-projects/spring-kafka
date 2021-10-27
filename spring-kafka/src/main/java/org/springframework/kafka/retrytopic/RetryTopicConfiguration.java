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
import org.springframework.kafka.support.EndpointHandlerMethod;

/**
 * Contains the provided configuration for the retryable topics.
 *
 * Should be created via the {@link RetryTopicConfigurationBuilder}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class RetryTopicConfiguration {

	private final List<DestinationTopic.Properties> destinationTopicProperties;

	private final AllowDenyCollectionManager<String> topicAllowListManager;

	private final EndpointHandlerMethod dltHandlerMethod;

	private final TopicCreation kafkaTopicAutoCreationConfig;

	private final ListenerContainerFactoryResolver.Configuration factoryResolverConfig;

	private final ListenerContainerFactoryConfigurer.Configuration factoryConfigurerConfig;

	RetryTopicConfiguration(List<DestinationTopic.Properties> destinationTopicProperties,
							EndpointHandlerMethod dltHandlerMethod,
							TopicCreation kafkaTopicAutoCreationConfig,
							AllowDenyCollectionManager<String> topicAllowListManager,
							ListenerContainerFactoryResolver.Configuration factoryResolverConfig,
							ListenerContainerFactoryConfigurer.Configuration factoryConfigurerConfig) {
		this.destinationTopicProperties = destinationTopicProperties;
		this.dltHandlerMethod = dltHandlerMethod;
		this.kafkaTopicAutoCreationConfig = kafkaTopicAutoCreationConfig;
		this.topicAllowListManager = topicAllowListManager;
		this.factoryResolverConfig = factoryResolverConfig;
		this.factoryConfigurerConfig = factoryConfigurerConfig;
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

	public ListenerContainerFactoryConfigurer.Configuration forContainerFactoryConfigurer() {
		return this.factoryConfigurerConfig;
	}

	public EndpointHandlerMethod getDltHandlerMethod() {
		return this.dltHandlerMethod;
	}

	public List<DestinationTopic.Properties> getDestinationTopicProperties() {
		return this.destinationTopicProperties;
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
