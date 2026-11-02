/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.test.hamcrest;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasPartition;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasTimestamp;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

/**
 * @author Biju Kunjummen
 *
 * @since 1.3
 */
public class KafkaMatchersTests {

	@Test
	public void testKeyMatcher() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 10, 1487694048607L,
				TimestampType.CREATE_TIME, 2, 3, "key1", "value1", new RecordHeaders(), Optional.empty());
		assertThat(record, hasKey("key1"));
		assertThat(record, hasValue("value1"));
		assertThat(record, hasPartition(0));
		assertThat(record, hasTimestamp(1487694048607L));
		assertThat(record, hasTimestamp(TimestampType.CREATE_TIME, 1487694048607L));
	}

	@Test
	public void noMatchOnTimestamp() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 10, 1487694048607L,
				TimestampType.CREATE_TIME, 2, 3, "key1", "value1", new RecordHeaders(), Optional.empty());

		assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> assertThat(record, hasTimestamp(123L)))
				.withMessageContaining(
						"Expected: a ConsumerRecord with timestamp of type: <CreateTime> and value: <123L>");
	}

}
