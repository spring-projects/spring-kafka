/*
 * Copyright 2002-2016 the original author or authors.
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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * {@code @Configuration} class that registers a {@link KafkaListenerAnnotationBeanPostProcessor}
 * bean capable of processing Spring's @{@link KafkaListener} annotation. Also register
 * a default {@link KafkaListenerEndpointRegistry}.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableKafka}
 * annotation.  See {@link EnableKafka} Javadoc for complete usage.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListenerEndpointRegistry
 * @see EnableKafka
 */
@Configuration
public class KafkaBootstrapConfiguration {

	@SuppressWarnings("rawtypes")
	@Bean(name = KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public KafkaListenerAnnotationBeanPostProcessor kafkaListenerAnnotationProcessor() {
		return new KafkaListenerAnnotationBeanPostProcessor();
	}

	@Bean(name = KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
	public KafkaListenerEndpointRegistry defaultKafkaListenerEndpointRegistry() {
		return new KafkaListenerEndpointRegistry();
	}

}
