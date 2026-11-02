/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.time.Duration;

import org.apache.kafka.clients.producer.Producer;

import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.util.Assert;

/**
 * Kafka resource holder, wrapping a Kafka producer. KafkaTransactionManager binds instances of this
 * class to the thread, for a given Kafka producer factory.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 */
public class KafkaResourceHolder<K, V> extends ResourceHolderSupport {

	private final Producer<K, V> producer;

	private final Duration closeTimeout;

	private boolean committed;

	/**
	 * Construct an instance for the producer.
	 * @param producer the producer.
	 * @param closeTimeout the close timeout.
	 */
	public KafkaResourceHolder(Producer<K, V> producer, Duration closeTimeout) {
		Assert.notNull(producer, "'producer' cannot be null");
		Assert.notNull(closeTimeout, "'closeTimeout' cannot be null");
		this.producer = producer;
		this.closeTimeout = closeTimeout;
	}

	public Producer<K, V> getProducer() {
		return this.producer;
	}

	public void commit() {
		if (!this.committed) {
			this.producer.commitTransaction();
			this.committed = true;
		}
	}

	public void close() {
		this.producer.close(this.closeTimeout);
	}

	public void rollback() {
		this.producer.abortTransaction();
	}

}
