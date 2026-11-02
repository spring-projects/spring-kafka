/*
 * Copyright 2016-present the original author or authors.
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
