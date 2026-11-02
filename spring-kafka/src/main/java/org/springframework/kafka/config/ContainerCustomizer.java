/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.config;

import org.springframework.kafka.listener.AbstractMessageListenerContainer;

/**
 * Called by the container factory after the container is created and configured.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <C> the container type.
 *
 * @author Gary Russell
 * @since 2.3.4
 *
 */
@FunctionalInterface
public interface ContainerCustomizer<K, V, C extends AbstractMessageListenerContainer<K, V>> {

	/**
	 * Configure the container.
	 * @param container the container.
	 */
	void configure(C container);

}
