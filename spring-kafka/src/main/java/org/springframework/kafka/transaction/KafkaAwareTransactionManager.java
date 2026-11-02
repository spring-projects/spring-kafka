/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.transaction;

import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A transaction manager that can provide a {@link ProducerFactory}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public interface KafkaAwareTransactionManager<K, V> extends PlatformTransactionManager {

	/**
	 * Get the producer factory.
	 * @return the producerFactory
	 */
	ProducerFactory<K, V> getProducerFactory();

}
