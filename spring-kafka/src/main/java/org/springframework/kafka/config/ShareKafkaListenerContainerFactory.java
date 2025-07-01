/*
 * Copyright 2025-present the original author or authors.
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

import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ShareKafkaMessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;

/**
 * A {@link KafkaListenerContainerFactory} implementation to create {@link ShareKafkaMessageListenerContainer}
 * instances for Kafka's share consumer model.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 * @since 4.0
 */
public class ShareKafkaListenerContainerFactory<K, V>
		extends AbstractShareKafkaListenerContainerFactory<ShareKafkaMessageListenerContainer<K, V>, K, V> {

	/**
	 * Construct an instance with the provided consumer factory.
	 * @param shareConsumerFactory the share consumer factory
	 */
	public ShareKafkaListenerContainerFactory(ShareConsumerFactory<K, V> shareConsumerFactory) {
		setShareConsumerFactory(shareConsumerFactory);
	}

	@Override
	protected ShareKafkaMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {
		TopicPartitionOffset[] topicPartitions = endpoint.getTopicPartitionsToAssign();
		if (topicPartitions != null && topicPartitions.length > 0) {
			return new ShareKafkaMessageListenerContainer<>(getShareConsumerFactory(), new ContainerProperties(topicPartitions));
		}
		else {
			Collection<String> topics = endpoint.getTopics();
			Assert.state(topics != null, "'topics' must not be null");
			if (!topics.isEmpty()) {
				return new ShareKafkaMessageListenerContainer<>(getShareConsumerFactory(),
						new ContainerProperties(topics.toArray(new String[0])));
			}
			else {
				return new ShareKafkaMessageListenerContainer<>(getShareConsumerFactory(),
						new ContainerProperties(endpoint.getTopicPattern()));
			}
		}
	}
}
