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
 * @author Endika Gutiérrez
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
	}

	/**
	 * Invoked after an attempt to send a message has failed.
	 * @param producerRecord the failed record
	 * @param exception the exception thrown
	 */
	default void onError(ProducerRecord<K, V> producerRecord, Exception exception) {
	}

}
