/*
 * Copyright 2014-2018 the original author or authors.
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

import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

/**
 * A {@link KafkaListenerContainerFactory} implementation to build a
 * {@link ConcurrentMessageListenerContainer}.
 * <p>
 * This should be the default for most users and a good transition paths for those that
 * are used to building such container definitions manually.
 *
 * This factory is primarily for building containers for {@code KafkaListener} annotated
 * methods but can also be used to create any container.
 *
 * Only containers for {@code KafkaListener} annotated methods are added to the
 * {@code KafkaListenerEndpointRegistry}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Murali Reddy
 */
public class ConcurrentKafkaListenerContainerFactory<K, V>
		extends AbstractKafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>, K, V> {

	private Integer concurrency;

	/**
	 * Specify the container concurrency.
	 * @param concurrency the number of consumers to create.
	 * @see ConcurrentMessageListenerContainer#setConcurrency(int)
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	@Override
	protected ConcurrentMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {
		Collection<TopicPartitionInitialOffset> topicPartitions = endpoint.getTopicPartitions();
		if (!topicPartitions.isEmpty()) {
			ContainerProperties properties = new ContainerProperties(
					topicPartitions.toArray(new TopicPartitionInitialOffset[topicPartitions.size()]));
			return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
		}
		else {
			Collection<String> topics = endpoint.getTopics();
			if (!topics.isEmpty()) {
				ContainerProperties properties = new ContainerProperties(topics.toArray(new String[topics.size()]));
				return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
			}
			else {
				ContainerProperties properties = new ContainerProperties(endpoint.getTopicPattern());
				return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
			}
		}
	}

	@Override
	protected void initializeContainer(ConcurrentMessageListenerContainer<K, V> instance, KafkaListenerEndpoint endpoint) {
		super.initializeContainer(instance, endpoint);
		if (endpoint.getConcurrency() != null) {
			instance.setConcurrency(endpoint.getConcurrency());
		}
		else if (this.concurrency != null) {
			instance.setConcurrency(this.concurrency);
		}
	}

}

