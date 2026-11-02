/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.support;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Result for a {@link java.util.concurrent.CompletableFuture} after a send.
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
