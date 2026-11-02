/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * Called by consumer factories to perform post processing on newly created consumers.
 *
 * @param <K> the key type.
 * @param <V> the value type
 *
 * @author Gary Russell
 * @since 2.5.3
 *
 */
public interface ConsumerPostProcessor<K, V> extends Function<Consumer<K, V>, Consumer<K, V>> {

}
