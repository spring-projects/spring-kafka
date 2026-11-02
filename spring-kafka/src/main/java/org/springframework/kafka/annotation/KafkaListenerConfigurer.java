/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.kafka.annotation;


import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;

/**
 * Optional interface to be implemented by Spring managed bean willing
 * to customize how Kafka listener endpoints are configured. Typically
 * used to define the default
 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory
 * KafkaListenerContainerFactory} to use or for registering Kafka endpoints
 * in a <em>programmatic</em> fashion as opposed to the <em>declarative</em>
 * approach of using the @{@link KafkaListener} annotation.
 *
 * <p>See @{@link EnableKafka} for detailed usage examples.
 *
 * @author Stephane Nicoll
 *
 * @see EnableKafka
 * @see org.springframework.kafka.config.KafkaListenerEndpointRegistrar
 */
public interface KafkaListenerConfigurer {

	/**
	 * Callback allowing a {@link org.springframework.kafka.config.KafkaListenerEndpointRegistry
	 * KafkaListenerEndpointRegistry} and specific {@link org.springframework.kafka.config.KafkaListenerEndpoint
	 * KafkaListenerEndpoint} instances to be registered against the given
	 * {@link KafkaListenerEndpointRegistrar}. The default
	 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory KafkaListenerContainerFactory}
	 * can also be customized.
	 * @param registrar the registrar to be configured
	 */
	void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar);

}
