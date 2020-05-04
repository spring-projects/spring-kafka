/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.kafka.core;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.kafka.KafkaException;

/**
 * Exceptions when producing.
 *
 * @author Gary Russell
 *
 */
@SuppressWarnings("serial")
public class KafkaProducerException extends KafkaException {

	private final ProducerRecord<?, ?> producerRecord;

	/**
	 * Construct an instance with the provided properties.
	 * @param failedProducerRecord the producer record.
	 * @param message the message.
	 * @param cause the cause.
	 */
	public KafkaProducerException(ProducerRecord<?, ?> failedProducerRecord, String message, Throwable cause) {
		super(message, cause);
		this.producerRecord = failedProducerRecord;
	}

	/**
	 * Return the failed producer record.
	 * @return the record.
	 * @deprecated in favor of {@link #getFailedProducerRecord()}
	 */
	@Deprecated
	public ProducerRecord<?, ?> getProducerRecord() {
		return this.producerRecord;
	}

	/**
	 * Return the failed producer record.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @return the record.
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public <K, V> ProducerRecord<K, V> getFailedProducerRecord() {
		return (ProducerRecord<K, V>) producerRecord;
	}

}
