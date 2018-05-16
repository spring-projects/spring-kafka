/*
 * Copyright 2002-2018 the original author or authors.
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
import java.util.regex.Pattern;

import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

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
	 * @param topicPartitions the topicPartitions.
	 * @return the container.
	 * @since 2.2
	 */
	C createContainer(Collection<TopicPartitionInitialOffset> topicPartitions);

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
