/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
