/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;

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
	void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt);

	/**
	 * Called after a failing record was successfully recovered.
	 * @param record the record.
	 * @param ex the exception.
	 */
	default void recovered(ConsumerRecord<?, ?> record, Exception ex) {
	}

	/**
	 * Called after a recovery attempt failed.
	 * @param record the record.
	 * @param original the original exception causing the recovery attempt.
	 * @param failure the exception thrown by the recoverer.
	 */
	default void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
	}

}
