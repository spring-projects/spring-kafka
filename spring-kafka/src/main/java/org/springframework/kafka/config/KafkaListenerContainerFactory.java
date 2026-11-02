/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.kafka.config;

import java.util.regex.Pattern;

import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;

/**
 * Factory for {@link MessageListenerContainer}s.
 *
 * @param <C> the {@link MessageListenerContainer} implementation type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @see KafkaListenerEndpoint
 */
public interface KafkaListenerContainerFactory<C extends MessageListenerContainer> {

	/**
	 * Create a {@link MessageListenerContainer} for the given {@link KafkaListenerEndpoint}.
	 * Containers created using this method are added to the listener endpoint registry.
	 * @param endpoint the endpoint to configure
	 * @return the created container
	 */
	C createListenerContainer(KafkaListenerEndpoint endpoint);

	/**
	 * Create and configure a container without a listener; used to create containers that
	 * are not used for KafkaListener annotations. Containers created using this method
	 * are not added to the listener endpoint registry.
	 * @param topicPartitions the topicPartitions to assign.
	 * @return the container.
	 * @since 2.3
	 */
	C createContainer(TopicPartitionOffset... topicPartitions);

	/**
	 * Create and configure a container without a listener; used to create containers that
	 * are not used for KafkaListener annotations. Containers created using this method
	 * are not added to the listener endpoint registry.
	 * @param topics the topics.
	 * @return the container.
	 * @since 2.2
	 */
	C createContainer(String... topics);

	/**
	 * Create and configure a container without a listener; used to create containers that
	 * are not used for KafkaListener annotations. Containers created using this method
	 * are not added to the listener endpoint registry.
	 * @param topicPattern the topicPattern.
	 * @return the container.
	 * @since 2.2
	 */
	C createContainer(Pattern topicPattern);

}
