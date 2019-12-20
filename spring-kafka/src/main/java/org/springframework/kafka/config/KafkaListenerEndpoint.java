/*
 * Copyright 2002-2019 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.lang.Nullable;

/**
 * Model for a Kafka listener endpoint. Can be used against a
 * {@link org.springframework.kafka.annotation.KafkaListenerConfigurer
 * KafkaListenerConfigurer} to register endpoints programmatically.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public interface KafkaListenerEndpoint {

	/**
	 * Return the id of this endpoint.
	 * @return the id of this endpoint. The id can be further qualified
	 * when the endpoint is resolved against its actual listener
	 * container.
	 * @see KafkaListenerContainerFactory#createListenerContainer
	 */
	String getId();

	/**
	 * Return the groupId of this endpoint - if present, overrides the
	 * {@code group.id} property of the consumer factory.
	 * @return the group id; may be null.
	 * @since 1.3
	 */
	String getGroupId();

	/**
	 * Return the group of this endpoint or null if not in a group.
	 * @return the group of this endpoint or null if not in a group.
	 */
	String getGroup();

	/**
	 * Return the topics for this endpoint.
	 * @return the topics for this endpoint.
	 */
	Collection<String> getTopics();

	/**
	 * Return the topicPartitions for this endpoint.
	 * @return the topicPartitions for this endpoint.
	 * @deprecated in favor of {@link #getTopicPartitionsToAssign()}.
	 */
	@Deprecated
	default Collection<org.springframework.kafka.support.TopicPartitionInitialOffset> getTopicPartitions() {
		return Arrays.stream(getTopicPartitionsToAssign())
				.map(org.springframework.kafka.support.TopicPartitionInitialOffset::fromTPO)
				.collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
	}

	/**
	 * Return the topicPartitions for this endpoint.
	 * @return the topicPartitions for this endpoint.
	 * @since 2.3
	 */
	TopicPartitionOffset[] getTopicPartitionsToAssign();

	/**
	 * Return the topicPattern for this endpoint.
	 * @return the topicPattern for this endpoint.
	 */
	Pattern getTopicPattern();


	/**
	 * Return the client id prefix for the container; it will be suffixed by
	 * '-n' to provide a unique id when concurrency is used.
	 * @return the client id prefix.
	 * @since 2.1.1
	 */
	String getClientIdPrefix();

	/**
	 * Return the concurrency for this endpoint's container.
	 * @return the concurrency.
	 * @since 2.2
	 */
	Integer getConcurrency();

	/**
	 * Return the autoStartup for this endpoint's container.
	 * @return the autoStartup.
	 * @since 2.2
	 */
	Boolean getAutoStartup();

	/**
	 * Get the consumer properties that will be merged with the consumer properties
	 * provided by the consumer factory; properties here will supersede any with the same
	 * name(s) in the consumer factory.
	 * {@code group.id} and {@code client.id} are ignored.
	 * @return the properties.
	 * @since 2.1.4
	 * @see org.apache.kafka.clients.consumer.ConsumerConfig
	 * @see #getGroupId()
	 * @see #getClientIdPrefix()
	 */
	@Nullable
	default Properties getConsumerProperties() {
		return null;
	}

	/**
	 * Setup the specified message listener container with the model
	 * defined by this endpoint.
	 * <p>This endpoint must provide the requested missing option(s) of
	 * the specified container to make it usable. Usually, this is about
	 * setting the {@code queues} and the {@code messageListener} to
	 * use but an implementation may override any default setting that
	 * was already set.
	 * @param listenerContainer the listener container to configure
	 * @param messageConverter the message converter - can be null
	 */
	void setupListenerContainer(MessageListenerContainer listenerContainer, MessageConverter messageConverter);

	/**
	 * When true, {@link Iterable} return results will be split into discrete records.
	 * @return true to split.
	 * @since 2.3.5
	 */
	boolean isSplitIterables();

}
