/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.test.context;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.TestContextAnnotationUtils;

/**
 * The {@link ContextCustomizerFactory} implementation to produce a
 * {@link EmbeddedKafkaContextCustomizer} if a {@link EmbeddedKafka} annotation
 * is present on the test class.
 *
 * @author Artem Bilan
 *
 * @since 1.3
 */
class EmbeddedKafkaContextCustomizerFactory implements ContextCustomizerFactory {

	@Override
	@Nullable
	public ContextCustomizer createContextCustomizer(Class<?> testClass,
			List<ContextConfigurationAttributes> configAttributes) {
		EmbeddedKafka embeddedKafka =
				TestContextAnnotationUtils.findMergedAnnotation(testClass, EmbeddedKafka.class);
		return embeddedKafka != null ? new EmbeddedKafkaContextCustomizer(embeddedKafka) : null;
	}

}
