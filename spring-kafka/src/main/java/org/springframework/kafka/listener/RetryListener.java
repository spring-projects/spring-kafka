/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jspecify.annotations.Nullable;

/**
 * A listener for retry activity.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
@FunctionalInterface
public interface RetryListener {

	/**
	 * Called after a delivery failed for a record.
	 * @param record the failed record.
	 * @param ex the exception.
	 * @param deliveryAttempt the delivery attempt.
	 */
	void failedDelivery(ConsumerRecord<?, ?> record, @Nullable Exception ex, int deliveryAttempt);

	/**
	 * Called after a failing record was successfully recovered.
	 * @param record the record.
	 * @param ex the exception.
	 */
	default void recovered(ConsumerRecord<?, ?> record, @Nullable Exception ex) {
	}

	/**
	 * Called after a recovery attempt failed.
	 * @param record the record.
	 * @param original the original exception causing the recovery attempt.
	 * @param failure the exception thrown by the recoverer.
	 */
	default void recoveryFailed(ConsumerRecord<?, ?> record, @Nullable Exception original, Exception failure) {
	}

	/**
	 * Called after a delivery failed for batch records.
	 * @param records the records.
	 * @param ex the exception.
	 * @param deliveryAttempt the delivery attempt, if available.
	 * @since 2.8.10
	 */
	default void failedDelivery(ConsumerRecords<?, ?> records, Exception ex, int deliveryAttempt) {
	}

	/**
	 * Called after a failing record was successfully recovered.
	 * @param records the record.
	 * @param ex the exception.
	 */
	default void recovered(ConsumerRecords<?, ?> records, Exception ex) {
	}

	/**
	 * Called after a recovery attempt failed.
	 * @param records the record.
	 * @param original the original exception causing the recovery attempt.
	 * @param failure the exception thrown by the recoverer.
	 */
	default void recoveryFailed(ConsumerRecords<?, ?> records, Exception original, Exception failure) {
	}

}
