/*
 * Copyright 2018-2019 the original author or authors.
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

import java.time.temporal.ValueRange;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.lang.Nullable;

/**
 * Track record processing failure counts.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
class FailedRecordTracker {

	private final ThreadLocal<Map<TopicPartition, FailedRecord>> failures = new ThreadLocal<>(); // intentionally not static

	private final BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer;

	private final int maxFailures;

	private final boolean noRetries;

	private final Log logger;

	FailedRecordTracker(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, int maxFailures, Log logger) {
		if (recoverer == null) {
			this.recoverer = (r, t) -> logger.error("Max failures (" + maxFailures + ") reached for: " + r, t);
		}
		else {
			this.recoverer = recoverer;
		}
		this.maxFailures = maxFailures;
		this.noRetries = ValueRange.of(0, 1).isValidIntValue(maxFailures);
		this.logger = logger;
	}

	boolean skip(ConsumerRecord<?, ?> record, Exception exception) {
		if (this.noRetries) {
			recover(record, exception);
			return true;
		}
		Map<TopicPartition, FailedRecord> map = this.failures.get();
		if (map == null) {
			this.failures.set(new HashMap<>());
			map = this.failures.get();
		}
		TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
		FailedRecord failedRecord = map.get(topicPartition);
		if (failedRecord == null || failedRecord.getOffset() != record.offset()) {
			failedRecord = new FailedRecord(record.offset());
			map.put(topicPartition, failedRecord);
			return false;
		}
		else if (this.maxFailures > 0 && failedRecord.incrementAndGet() >= this.maxFailures) {
			recover(record, exception);
			map.remove(topicPartition);
			if (map.isEmpty()) {
				this.failures.remove();
			}
			return true;
		}
		else {
			return false;
		}
	}

	private void recover(ConsumerRecord<?, ?> record, Exception exception) {
		try {
			this.recoverer.accept(record, exception);
		}
		catch (Exception ex) {
			this.logger.error("Recoverer threw exception", ex);
		}
	}

	void clearThreadState() {
		this.failures.remove();
	}

	private static final class FailedRecord {

		private final long offset;

		private int count;

		FailedRecord(long offset) {
			this.offset = offset;
			this.count = 1;
		}

		long getOffset() {
			return this.offset;
		}

		private int incrementAndGet() {
			return ++this.count;
		}

	}

}
