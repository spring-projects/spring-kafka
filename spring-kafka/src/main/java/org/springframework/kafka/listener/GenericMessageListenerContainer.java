/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

/**
 * Generic message listener container; adds parameters.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public interface GenericMessageListenerContainer<K, V> extends MessageListenerContainer {

}
