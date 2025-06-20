/*
 * Copyright 2021-present the original author or authors.
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

import java.nio.ByteBuffer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.KafkaHeaders;

/**
 * The DeliveryAttemptAwareRetryListener class for {@link RetryListener} implementations.
 * The DeliveryAttemptAwareRetryListener adds the {@link KafkaHeaders}.DELIVERY_ATTEMPT header
 * to the record's headers when batch records fail and are retried.
 * Note that DeliveryAttemptAwareRetryListener modifies the headers of the original record.
 *
 * @author Sanghyeok An
 * @since 3.3
 */

public class DeliveryAttemptAwareRetryListener implements RetryListener {

	@Override
	public void failedDelivery(ConsumerRecord<?, ?> record, @Nullable Exception ex, int deliveryAttempt) {
		// Pass
	}

	/**
	 * Invoke after delivery failure for batch records.
	 * If the {@link KafkaHeaders}.DELIVERY_ATTEMPT header already exists in the {@link ConsumerRecord}'s headers,
	 * it will be removed. Then, the provided `deliveryAttempt` is added to the {@link ConsumerRecord}'s headers.
	 * @param records the records.
	 * @param ex the exception.
	 * @param deliveryAttempt the delivery attempt, if available.
	 */
	@Override
	public void failedDelivery(ConsumerRecords<?, ?> records, Exception ex, int deliveryAttempt) {
		for (ConsumerRecord<?, ?> record : records) {
			record.headers().remove(KafkaHeaders.DELIVERY_ATTEMPT);

			byte[] buff = new byte[4]; // NOSONAR (magic #)
			ByteBuffer bb = ByteBuffer.wrap(buff);
			bb.putInt(deliveryAttempt);
			record.headers().add(new RecordHeader(KafkaHeaders.DELIVERY_ATTEMPT, buff));
		}
	}

}
