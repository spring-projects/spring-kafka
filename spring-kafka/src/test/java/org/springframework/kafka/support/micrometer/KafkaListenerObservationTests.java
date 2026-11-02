/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.support.micrometer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.micrometer.KafkaListenerObservation.DefaultKafkaListenerObservationConvention;

/**
 * @author Christian Fredriksson
 */
public class KafkaListenerObservationTests {

	@Test
	void lowCardinalityKeyValues() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context = new KafkaRecordReceiverContext(record, "listener", () -> null);
		DefaultKafkaListenerObservationConvention.INSTANCE.getLowCardinalityKeyValues(context);
	}

	@Test
	void highCardinalityKeyValues() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context = new KafkaRecordReceiverContext(record, "listener", () -> null);
		DefaultKafkaListenerObservationConvention.INSTANCE.getHighCardinalityKeyValues(context);
	}
}
