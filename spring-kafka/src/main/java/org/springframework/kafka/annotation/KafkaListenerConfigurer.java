/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.annotation;


import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;

/**
 * Optional interface to be implemented by Spring managed bean willing
 * to customize how Kafka listener endpoints are configured. Typically
 * used to defined the default
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
