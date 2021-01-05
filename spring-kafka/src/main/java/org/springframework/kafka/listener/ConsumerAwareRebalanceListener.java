/*
 * Copyright 2017-2021 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.Collection;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;

/**
 * A rebalance listener that provides access to the consumer object. Starting with version
 * 2.1.5, as a convenience, default no-op implementations are provided for all methods,
 * allowing the user to implement just those (s)he is interested in.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public interface ConsumerAwareRebalanceListener extends ConsumerRebalanceListener {

	/**
	 * {@link LogAccessor} for use in default methods.
	 */
	LogAccessor logger = new LogAccessor(LogFactory.getLog(ConsumerAwareRebalanceListener.class));

	/**
	 * The same as {@link #onPartitionsRevoked(Collection)} with the additional consumer
	 * parameter. It is invoked by the container before any pending offsets are committed.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		try {
			onPartitionsRevoked(partitions);
		}
		catch (Exception e) { // NOSONAR
			logger.debug(e, "User method threw exception");
		}
	}

	/**
	 * The same as {@link #onPartitionsRevoked(Collection)} with the additional consumer
	 * parameter. It is invoked by the container after any pending offsets are committed.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
	}

	/**
	 * The same as {@link #onPartitionsLost(Collection)} with an additional consumer parameter.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 * @since 2.4
	 */
	default void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		try {
			onPartitionsLost(partitions);
		}
		catch (Exception e) { // NOSONAR
			logger.debug(e, "User method threw exception");
		}
	}

	/**
	 * The same as {@link #onPartitionsAssigned(Collection)} with the additional consumer
	 * parameter.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		try {
			onPartitionsAssigned(partitions);
		}
		catch (Exception e) { // NOSONAR
			logger.debug(e, "User method threw exception");
		}
	}

	@Override
	default void onPartitionsRevoked(Collection<TopicPartition> partitions) {
	}

	@Override
	default void onPartitionsAssigned(Collection<TopicPartition> partitions) {
	}

	@Override
	default void onPartitionsLost(Collection<TopicPartition> partitions) {
	}

}
