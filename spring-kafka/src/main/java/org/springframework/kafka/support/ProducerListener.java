/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.kafka.support;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Listener for handling outbound Kafka messages. Exactly one of its methods will be invoked, depending on whether
 * the write has been acknowledged or not.
 *
 * Its main goal is to provide a stateless singleton delegate for {@link org.apache.kafka.clients.producer.Callback}s,
 * which, in all but the most trivial cases, requires creating a separate instance per message.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Endika Guti?rrez
 *
 * @see org.apache.kafka.clients.producer.Callback
 */
public interface ProducerListener<K, V> {

	/**
	 * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker).
	 * @param producerRecord the actual sent record
	 * @param recordMetadata the result of the successful send operation
	 */
	default void onSuccess(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
		onSuccess(producerRecord.topic(), producerRecord.partition(),
				producerRecord.key(), producerRecord.value(), recordMetadata);
	}

	/**
	 * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker).
	 * If the method receiving the ProducerRecord is overridden, this method won't be called
	 * @param topic the destination topic
	 * @param partition the destination partition (could be null)
	 * @param key the key of the outbound message
	 * @param value the payload of the outbound message
	 * @param recordMetadata the result of the successful send operation
	 * @deprecated in favor of {@link #onSuccess(ProducerRecord, RecordMetadata)}.
	 */
	@Deprecated
	default void onSuccess(String topic, Integer partition, K key, V value, RecordMetadata recordMetadata) {
	}

	/**
	 * Invoked after an attempt to send a message has failed.
	 * @param producerRecord the failed record
	 * @param exception the exception thrown
	 */
	default void onError(ProducerRecord<K, V> producerRecord, Exception exception) {
		onError(producerRecord.topic(), producerRecord.partition(),
				producerRecord.key(), producerRecord.value(), exception);
	}

	/**
	 * Invoked after an attempt to send a message has failed.
	 * If the method receiving the ProducerRecord is overridden, this method won't be called
	 * @param topic the destination topic
	 * @param partition the destination partition (could be null)
	 * @param key the key of the outbound message
	 * @param value the payload of the outbound message
	 * @param exception the exception thrown
	 * @deprecated in favor of {@link #onError(ProducerRecord, Exception)}.
	 */
	@Deprecated
	default void onError(String topic, Integer partition, K key, V value, Exception exception) {
	}

	/**
	 * Return true if this listener is interested in success as well as failure.
	 * @deprecated the result of this method will be ignored.
	 * @return true to express interest in successful sends.
	 */
	@Deprecated
	default boolean isInterestedInSuccess() {
		return false;
	}

}
