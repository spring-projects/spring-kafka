/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Track record processing failure counts.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class FailedRecordTracker<K, V> {

	private final ThreadLocal<FailedRecord> failures = new ThreadLocal<>(); // intentionally not static

	private final BiConsumer<ConsumerRecord<K, V>, Exception> recoverer;

	private final int maxFailures;

	public FailedRecordTracker(BiConsumer<ConsumerRecord<K, V>, Exception> recoverer, int maxFailures, Log logger) {
		if (recoverer == null) {
			this.recoverer = (r, t) -> logger.error("Max failures (" + maxFailures + ") reached for: " + r, t);
		}
		else {
			this.recoverer = recoverer;
		}
		this.maxFailures = maxFailures;
	}

	public boolean skip(ConsumerRecord<K, V> record, Exception exception) {
		FailedRecord failedRecord = this.failures.get();
		if (failedRecord == null || !failedRecord.getTopic().equals(record.topic())
				|| failedRecord.getPartition() != record.partition() || failedRecord.getOffset() != record.offset()) {
			this.failures.set(new FailedRecord(record.topic(), record.partition(), record.offset()));
			return false;
		}
		else {
			if (failedRecord.incrementAndGet() >= this.maxFailures) {
				this.recoverer.accept(record, exception);
				return true;
			}
			return false;
		}
	}

	public void clearThreadState() {
		this.failures.remove();
	}

	private static final class FailedRecord {

		private final String topic;

		private final int partition;

		private final long offset;

		private int count;

		FailedRecord(String topic, int partition, long offset) {
			this.topic = topic;
			this.partition = partition;
			this.offset = offset;
			this.count = 1;
		}

		private String getTopic() {
			return this.topic;
		}

		private int getPartition() {
			return this.partition;
		}

		private long getOffset() {
			return this.offset;
		}

		private int incrementAndGet() {
			return ++this.count;
		}

	}

}
