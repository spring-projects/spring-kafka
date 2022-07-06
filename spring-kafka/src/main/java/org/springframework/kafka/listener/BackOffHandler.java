/*
 * Copyright 2022 the original author or authors.
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

import org.apache.kafka.common.TopicPartition;

import org.springframework.lang.Nullable;

/**
 * Handler for the provided back off time, listener container and exception.
 * Also supports back off for individual partitions.
 *
 * @author Jan Marincek
 * @author Gary Russell
 * @since 2.9
 */
public interface BackOffHandler {

	/**
	 * Perform the next back off.
	 * @param container the container.
	 * @param exception the exception.
	 * @param nextBackOff the next back off.
	 */
	default void onNextBackOff(@Nullable MessageListenerContainer container, Exception exception, long nextBackOff) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Perform the next back off for a partition.
	 * @param container the container.
	 * @param partition the partition.
	 * @param nextBackOff the next back off.
	 */
	default void onNextBackOff(@Nullable MessageListenerContainer container, TopicPartition partition,
			long nextBackOff) {

		throw new UnsupportedOperationException();
	}

}
