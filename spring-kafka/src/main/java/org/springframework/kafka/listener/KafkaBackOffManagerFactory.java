/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

/**
 *
 * Creates a {@link KafkaBackOffManagerFactory} instance.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @see KafkaConsumerBackoffManager
 */
public interface KafkaBackOffManagerFactory {

	KafkaConsumerBackoffManager create();

}
