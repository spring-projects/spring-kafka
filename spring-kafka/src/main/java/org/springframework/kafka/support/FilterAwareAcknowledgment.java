/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.support;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An extension of {@link Acknowledgment} that allows tracking of filtered records
 * for offset commit management with {@link org.springframework.kafka.listener.ContainerProperties.AckMode#RECORD_FILTERED}.
 * When a record is marked as filtered, its offset will be excluded from commits to
 * ensure the record can be reprocessed if needed.
 *
 * @author Spring Kafka Team
 * @since 3.1
 */
public interface FilterAwareAcknowledgment extends Acknowledgment {

	/**
	 * Mark a record as filtered. When using RECORD_FILTERED acknowledgment mode,
	 * offsets for filtered records will be excluded from commits.
	 * @param record the filtered record
	 */
	void markFiltered(ConsumerRecord<?, ?> record);

	/**
	 * Mark multiple records as filtered.
	 * @param records the filtered records
	 */
	default void markFiltered(List<? extends ConsumerRecord<?, ?>> records) {
		records.forEach(this::markFiltered);
	}

}