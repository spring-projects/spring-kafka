/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * Result for a Listenablefuture after a send.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class SendResult<K, V> {

	private final ProducerRecord<K, V> producerRecord;

	private final RecordMetadata recordMetadata;

	public SendResult(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
		this.producerRecord = producerRecord;
		this.recordMetadata = recordMetadata;
	}

	public ProducerRecord<K, V> getProducerRecord() {
		return this.producerRecord;
	}

	public RecordMetadata getRecordMetadata() {
		return this.recordMetadata;
	}

	@Override
	public String toString() {
		return "SendResult [producerRecord=" + this.producerRecord + ", recordMetadata=" + this.recordMetadata + "]";
	}

}
