/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.annotation;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

/**
 * A {@link DeferredImportSelector} implementation with the lowest order to import a
 * {@link KafkaBootstrapConfiguration} as late as possible.
 *
 * @author Artem Bilan
 *
 * @since 2.3
 */
@Order
public class KafkaListenerConfigurationSelector implements DeferredImportSelector {

	@Override
	public String[] selectImports(AnnotationMetadata importingClassMetadata) {
		return new String[] { KafkaBootstrapConfiguration.class.getName() };
	}

}
