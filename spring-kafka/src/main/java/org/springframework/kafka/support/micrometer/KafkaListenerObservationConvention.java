/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.support.micrometer;

import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Kafka listener key values.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public interface KafkaListenerObservationConvention extends ObservationConvention<KafkaRecordReceiverContext> {

	@Override
	default boolean supportsContext(Context context) {
		return context instanceof KafkaRecordReceiverContext;
	}

	@Override
	default String getName() {
		return "spring.kafka.listener";
	}

}
