/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.support.micrometer;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.micrometer.KafkaListenerObservation.DefaultKafkaListenerObservationConvention;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Christian Fredriksson
 * @author Hakaze Arimu
 */
public class KafkaListenerObservationTests {

	@Test
	void lowCardinalityKeyValuesAlwaysIncludeOptionalTags() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context = new KafkaRecordReceiverContext(record, "listener", () -> null);
		KeyValues keyValues = DefaultKafkaListenerObservationConvention.INSTANCE.getLowCardinalityKeyValues(context);
		assertThat(keyValues.stream().map(KeyValue::getKey))
				.containsExactlyInAnyOrder(
						"spring.kafka.listener.id",
						"messaging.system",
						"messaging.operation",
						"messaging.source.name",
						"messaging.source.kind",
						"messaging.kafka.consumer.group");
		assertThat(keyValues.stream()
				.filter(kv -> "messaging.kafka.consumer.group".equals(kv.getKey()))
				.map(KeyValue::getValue)
				.findFirst())
				.contains(KeyValue.NONE_VALUE);
	}

	@Test
	void lowCardinalityKeyValuesIncludeGroupWhenPresent() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context =
				new KafkaRecordReceiverContext(record, "listener", "client-1", "group-1", () -> null);
		KeyValues keyValues = DefaultKafkaListenerObservationConvention.INSTANCE.getLowCardinalityKeyValues(context);
		assertThat(keyValues.stream()
				.filter(kv -> "messaging.kafka.consumer.group".equals(kv.getKey()))
				.map(KeyValue::getValue)
				.findFirst())
				.contains("group-1");
	}

	@Test
	void highCardinalityKeyValuesAlwaysIncludeOptionalTags() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context = new KafkaRecordReceiverContext(record, "listener", () -> null);
		KeyValues keyValues = DefaultKafkaListenerObservationConvention.INSTANCE.getHighCardinalityKeyValues(context);
		assertThat(keyValues.stream().map(KeyValue::getKey))
				.containsExactlyInAnyOrder(
						"messaging.kafka.source.partition",
						"messaging.kafka.message.offset",
						"messaging.kafka.client_id",
						"messaging.consumer.id");
		assertThat(keyValues.stream()
				.filter(kv -> "messaging.kafka.client_id".equals(kv.getKey())
						|| "messaging.consumer.id".equals(kv.getKey()))
				.map(KeyValue::getValue))
				.containsOnly(KeyValue.NONE_VALUE);
	}
}
