/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.core.log.LogAccessor;

/**
 * A strategy interface for determining the acknowledgment action when a share
 * consumer record fails processing. Implementations decide whether to
 * {@link AcknowledgeType#ACCEPT ACCEPT}, {@link AcknowledgeType#RELEASE RELEASE},
 * or {@link AcknowledgeType#REJECT REJECT} the failed record.
 *
 * <p>Built-in implementations: {@link #REJECTING} (log and REJECT, default),
 * {@link #RELEASING} (log and RELEASE for redelivery). Users can provide custom
 * implementations or use a lambda.
 *
 * @author Soby Chacko
 *
 * @since 4.1
 *
 * @see ShareKafkaMessageListenerContainer
 */
@FunctionalInterface
public interface ShareConsumerRecordRecoverer {

	/**
	 * Logger used by built-in implementations.
	 */
	LogAccessor LOGGER = new LogAccessor(ShareConsumerRecordRecoverer.class);

	/**
	 * Recoverer that logs the failure and REJECTs the record (archived, no redelivery).
	 * This is the default when none is configured.
	 */
	ShareConsumerRecordRecoverer REJECTING = (record, ex) -> {
		LOGGER.error(ex, () -> "Share consumer record processing failed; rejecting record from "
				+ record.topic() + "-" + record.partition() + "@" + record.offset());
		return AcknowledgeType.REJECT;
	};

	/**
	 * Recoverer that logs the failure and RELEASEs the record (available for redelivery).
	 */
	ShareConsumerRecordRecoverer RELEASING = (record, ex) -> {
		LOGGER.error(ex, () -> "Share consumer record processing failed; releasing record from "
				+ record.topic() + "-" + record.partition() + "@" + record.offset());
		return AcknowledgeType.RELEASE;
	};

	/**
	 * Determine the acknowledgment action for a failed record.
	 * @param record the record that failed processing
	 * @param exception the exception thrown during processing
	 * @return the {@link AcknowledgeType} to use — typically REJECT or RELEASE
	 */
	AcknowledgeType recover(ConsumerRecord<?, ?> record, Exception exception);

}
