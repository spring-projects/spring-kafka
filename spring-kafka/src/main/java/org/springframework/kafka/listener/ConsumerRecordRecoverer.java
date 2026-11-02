/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A {@link BiConsumer} extension for recovering consumer records.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
@FunctionalInterface
public interface ConsumerRecordRecoverer extends BiConsumer<ConsumerRecord<?, ?>, Exception> {

}
