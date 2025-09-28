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
 * A message listener for share consumer containers with acknowledgment support.
 * <p>
 * This interface provides access to both the {@link ShareConsumer} instance and acknowledgment
 * capabilities. The acknowledgment parameter behavior depends on the container's
 * acknowledgment mode:
 * <ul>
 * <li><strong>Explicit mode</strong>: The acknowledgment parameter is non-null and must
 * be used to acknowledge each record</li>
 * <li><strong>Implicit mode</strong>: The acknowledgment parameter is null and records
 * are automatically acknowledged</li>
 * </ul>
 * <p>
 * This is the primary listener interface for share consumers when you need access
 * to the ShareConsumer instance or need explicit acknowledgment control.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author Soby Chacko
 *
 * @since 4.0
 *
 * @see ShareAcknowledgment
 * @see ShareConsumer
 */
@FunctionalInterface
public interface AcknowledgingShareConsumerAwareMessageListener<K, V> extends GenericMessageListener<ConsumerRecord<K, V>> {

	/**
	 * Invoked with data from kafka, an acknowledgment, and provides access to the consumer.
	 * When explicit acknowledgment mode is used, the acknowledgment parameter will be non-null
	 * and must be used to acknowledge the record. When implicit acknowledgment mode is used,
	 * the acknowledgment parameter will be null.
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
