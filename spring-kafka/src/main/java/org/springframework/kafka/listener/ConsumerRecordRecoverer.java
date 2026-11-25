/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
