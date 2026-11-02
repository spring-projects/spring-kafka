/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ShareConsumerRecordRecoverer} built-in implementations.
 *
 * @author Soby Chacko
 * @since 4.1
 */
class ShareConsumerRecordRecovererTests {

	private static final ConsumerRecord<String, String> RECORD =
			new ConsumerRecord<>("test-topic", 1, 42L, "key", "value");

	@Test
	void rejectingReturnsReject() {
		assertThat(ShareConsumerRecordRecoverer.REJECTING.recover(RECORD, new RuntimeException("test")))
				.isEqualTo(AcknowledgeType.REJECT);
	}

	@Test
	void releasingReturnsRelease() {
		assertThat(ShareConsumerRecordRecoverer.RELEASING.recover(RECORD, new RuntimeException("test")))
				.isEqualTo(AcknowledgeType.RELEASE);
	}

}
