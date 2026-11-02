/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.support.micrometer;

import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Kafka template key values.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public interface KafkaTemplateObservationConvention extends ObservationConvention<KafkaRecordSenderContext> {

	@Override
	default boolean supportsContext(Context context) {
		return context instanceof KafkaRecordSenderContext;
	}

	@Override
	default String getName() {
		return "spring.kafka.template";
	}

}
