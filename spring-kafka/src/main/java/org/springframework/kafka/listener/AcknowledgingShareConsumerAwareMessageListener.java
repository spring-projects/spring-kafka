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
 * {@link ContainerProperties.ShareAckMode}:
 * <ul>
 * <li><strong>MANUAL</strong>: The acknowledgment is non-null; the listener must call
 * {@link org.springframework.kafka.support.ShareAcknowledgment#acknowledge()},
 * {@link org.springframework.kafka.support.ShareAcknowledgment#release()}, or
 * {@link org.springframework.kafka.support.ShareAcknowledgment#reject()} for every record</li>
 * <li><strong>EXPLICIT</strong>: The acknowledgment is null; the container sends ACCEPT
 * automatically on success and delegates errors to the {@link ShareConsumerRecordRecoverer}</li>
 * <li><strong>IMPLICIT</strong>: The acknowledgment is null; the broker auto-accepts all records</li>
 * </ul>
 * <p>
 * This is the primary listener interface for share consumers when you need access
 * to the {@link ShareConsumer} instance or need listener-managed acknowledgment
 * control ({@code ShareAckMode.MANUAL}).
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
	 * Invoked with data from Kafka, an acknowledgment, and provides access to the consumer.
	 * The acknowledgment is non-null only in {@link ContainerProperties.ShareAckMode#MANUAL} mode;
	 * it is null in {@code EXPLICIT} and {@code IMPLICIT} modes.
	 * @param data the data to be processed.
	 * @param acknowledgment the acknowledgment, or {@code null} if not in MANUAL mode.
	 * @param consumer the consumer.
	 */
	void onShareRecord(ConsumerRecord<K, V> data, @Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer);

	@Override
	default void onMessage(ConsumerRecord<K, V> data) {
		throw new UnsupportedOperationException("Container should never call this");
	}
}
