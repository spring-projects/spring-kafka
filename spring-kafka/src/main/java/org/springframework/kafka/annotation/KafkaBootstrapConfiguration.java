/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.kafka.annotation;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * An {@link ImportBeanDefinitionRegistrar} class that registers a {@link KafkaListenerAnnotationBeanPostProcessor}
 * bean capable of processing Spring's @{@link KafkaListener} annotation. Also register
 * a default {@link KafkaListenerEndpointRegistry}.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableKafka}
 * annotation.  See {@link EnableKafka} Javadoc for complete usage.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListenerEndpointRegistry
 * @see EnableKafka
 */
public class KafkaBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		if (!registry.containsBeanDefinition(
				KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {

			registry.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(KafkaListenerAnnotationBeanPostProcessor.class));
		}

		if (!registry.containsBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)) {
			registry.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
					new RootBeanDefinition(KafkaListenerEndpointRegistry.class));
		}
	}

}
