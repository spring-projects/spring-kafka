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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.ShareAcknowledgment;

/**
 * A message listener for share consumer containers that supports explicit acknowledgment.
 * <p>
 * This interface provides both access to the ShareConsumer and explicit acknowledgment
 * capabilities. When used with explicit acknowledgment mode, the acknowledgment parameter
 * will be non-null and must be used to acknowledge each record. In implicit mode,
 * the acknowledgment parameter will be null and records are auto-acknowledged.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Soby Chacko
 * @since 4.0
 * @see ShareAcknowledgment
 * @see ContainerProperties.ShareAcknowledgmentMode
 */
@FunctionalInterface
public interface AcknowledgingShareConsumerAwareMessageListener<K, V> extends GenericMessageListener<ConsumerRecord<K, V>> {

	/**
	 * Invoked with data from kafka, an acknowledgment, and provides access to the consumer.
	 * When explicit acknowledgment mode is used, the acknowledgment parameter will be non-null
	 * and must be used to acknowledge the record. When implicit acknowledgment mode is used,
	 * the acknowledgment parameter will be null.
	 *
	 * @param data the data to be processed.
	 * @param acknowledgment the acknowledgment (nullable in implicit mode).
	 * @param consumer the consumer.
	 */
	void onShareRecord(ConsumerRecord<K, V> data, @Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer);

	@Override
	default void onMessage(ConsumerRecord<K, V> data) {
		throw new UnsupportedOperationException("Container should never call this");
	}
}
