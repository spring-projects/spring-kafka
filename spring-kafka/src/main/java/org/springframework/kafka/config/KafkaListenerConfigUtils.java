/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.config;

/**
 * Configuration constants for internal sharing across subpackages.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 * @author Tomaz Fernandes
 */
public final class KafkaListenerConfigUtils {

	private KafkaListenerConfigUtils() {
	}

	/**
	 * The bean name of the internally managed Kafka listener annotation processor.
	 */
	public static final String KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
			"org.springframework.kafka.config.internalKafkaListenerAnnotationProcessor";

	/**
	 * The bean name of the internally managed Kafka listener endpoint registry.
	 */
	public static final String KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
			"org.springframework.kafka.config.internalKafkaListenerEndpointRegistry";

	/**
	 * The bean name of the internally managed Kafka consumer back off manager.
	 */
	public static final String KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME =
			"org.springframework.kafka.config.internalKafkaConsumerBackOffManager";

}
