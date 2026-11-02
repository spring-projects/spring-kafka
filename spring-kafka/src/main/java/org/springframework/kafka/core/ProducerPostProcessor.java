/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.function.Function;

import org.apache.kafka.clients.producer.Producer;

/**
 * Called by producer factories to perform post processing on newly created producers.
 *
 * @param <K> the key type.
 * @param <V> the value type
 *
 * @author Gary Russell
 * @since 2.5.3
 *
 */
public interface ProducerPostProcessor<K, V> extends Function<Producer<K, V>, Producer<K, V>> {

}
