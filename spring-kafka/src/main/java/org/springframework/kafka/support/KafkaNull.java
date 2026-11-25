/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.support;

/**
 * This class represents NULL Kafka payload.
 *
 * @author Dariusz Szablinski
 * @author Gary Russell
 * @since 1.0.3
 */
public final class KafkaNull {

	/**
	 * Instance of KafkaNull.
	 */
	public static final KafkaNull INSTANCE = new KafkaNull();

	private KafkaNull() {
	}

}
