/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.kafka.config;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;

/**
 * A {@link KafkaListenerContainerFactory} implementation to build a
 * {@link ConcurrentMessageListenerContainer}.
 * <p>
 * This should be the default for most users and a good transition paths for those that
 * are used to building such container definitions manually.
 * <p>
 * This factory is primarily for building containers for {@code KafkaListener} annotated
 * methods but can also be used to create any container.
 * <p>
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

	private @Nullable Integer concurrency;

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
		TopicPartitionOffset[] topicPartitions = endpoint.getTopicPartitionsToAssign();
		if (topicPartitions != null && topicPartitions.length > 0) {
			ContainerProperties properties = new ContainerProperties(topicPartitions);
			return new ConcurrentMessageListenerContainer<>(getConsumerFactory(), properties);
		}
		else {
			Collection<String> topics = endpoint.getTopics();
			Assert.state(topics != null, "'topics' must not be null");
			if (!topics.isEmpty()) { // NOSONAR
				ContainerProperties properties = new ContainerProperties(topics.toArray(new String[0]));
				return new ConcurrentMessageListenerContainer<>(getConsumerFactory(), properties);
			}
			else {
				ContainerProperties properties = new ContainerProperties(endpoint.getTopicPattern()); // NOSONAR
				return new ConcurrentMessageListenerContainer<>(getConsumerFactory(), properties);
			}
		}
	}

	@Override
	protected void initializeContainer(ConcurrentMessageListenerContainer<K, V> instance,
			KafkaListenerEndpoint endpoint) {

		super.initializeContainer(instance, endpoint);
		Integer conc = endpoint.getConcurrency();
		if (conc != null) {
			instance.setConcurrency(conc);
		}
		else if (this.concurrency != null) {
			instance.setConcurrency(this.concurrency);
		}
	}

}

