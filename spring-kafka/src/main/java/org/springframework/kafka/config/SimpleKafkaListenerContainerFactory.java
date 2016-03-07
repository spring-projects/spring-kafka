/*
 * Copyright 2014-2016 the original author or authors.
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

import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer.ContainerOffsetResetStrategy;
import org.springframework.kafka.listener.KafkaListenerContainerFactory;
import org.springframework.kafka.listener.KafkaListenerEndpoint;

/**
 * A {@link KafkaListenerContainerFactory} implementation to build a regular
 * {@link ConcurrentMessageListenerContainer}.
 *
 * <p>This should be the default for most users and a good transition paths
 * for those that are used to build such container definition manually.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 */
public class SimpleKafkaListenerContainerFactory<K, V>
		extends AbstractKafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>, K, V> {

	private Integer concurrency;

	private Long recentOffset;

	private ContainerOffsetResetStrategy resetStrategy;

	/**
	 * @param concurrency the number of consumers to create.
	 * @see ConcurrentMessageListenerContainer#setConcurrency(int)
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * @param recentOffset the recent offset.
	 * @see ConcurrentMessageListenerContainer#setRecentOffset(long)
	 */
	public void setRecentOffset(Long recentOffset) {
		this.recentOffset = recentOffset;
	}

	/**
	 * @param resetStrategy the reset strategy
	 * @see ConcurrentMessageListenerContainer#setResetStrategy(ContainerOffsetResetStrategy)
	 */
	public void setResetStrategy(ContainerOffsetResetStrategy resetStrategy) {
		this.resetStrategy = resetStrategy;
	}

	@Override
	protected ConcurrentMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {
		Collection<TopicPartition> topicPartitions = endpoint.getTopicPartitions();
		if (!topicPartitions.isEmpty()) {
			return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(),
					topicPartitions.toArray(new TopicPartition[topicPartitions.size()]));
		}
		else {
			Collection<String> topics = endpoint.getTopics();
			if (!topics.isEmpty()) {
				return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(),
						topics.toArray(new String[topics.size()]));
			}
			else {
				return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), endpoint.getTopicPattern());
			}
		}
	}

	@Override
	protected void initializeContainer(ConcurrentMessageListenerContainer<K, V> instance) {
		super.initializeContainer(instance);

		if (this.concurrency != null) {
			instance.setConcurrency(this.concurrency);
		}
		if (this.recentOffset != null) {
			instance.setRecentOffset(this.recentOffset);
		}
		if (this.resetStrategy != null) {
			instance.setResetStrategy(this.resetStrategy);
		}
	}

}
