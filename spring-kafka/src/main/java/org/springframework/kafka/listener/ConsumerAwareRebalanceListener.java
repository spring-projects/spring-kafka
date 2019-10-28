/*
 * Copyright 2017-2019 the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

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
	 * The same as {@link #onPartitionsRevoked(Collection)} with the additional consumer
	 * parameter. It is invoked by the container before any pending offsets are committed.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		// do nothing
	}

	/**
	 * The same as {@link #onPartitionsRevoked(Collection)} with the additional consumer
	 * parameter. It is invoked by the container after any pending offsets are committed.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		// do nothing
	}

	/**
	 * The same as {@link #onPartitionsLost(Collection)} with an additional consumer parameter.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 * @since 2.4
	 */
	default void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		// do nothing
	}

	/**
	 * The same as {@link #onPartitionsAssigned(Collection)} with the additional consumer
	 * parameter.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	default void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		// do nothing
	}

	@Override
	default void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		throw new UnsupportedOperationException("Listener container should never call this");
	}

	@Override
	default void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		throw new UnsupportedOperationException("Listener container should never call this");
	}

	@Override
	default void onPartitionsLost(Collection<TopicPartition> partitions) {
		throw new UnsupportedOperationException("Listener container should never call this");
	}

}
