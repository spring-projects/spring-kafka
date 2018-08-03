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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link AfterRollbackProcessor}. Seeks all
 * topic/partitions so the records will be re-fetched, including the failed
 * record.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
public class DefaultAfterRollbackProcessor<K, V> implements AfterRollbackProcessor<K, V> {

	/**
	 * The number of times a topic/partition/offset can fail before being rejected.
	 */
	public static final int DEFAULT_MAX_FAILURES = 10;

	private static final Log logger = LogFactory.getLog(DefaultAfterRollbackProcessor.class);

	private static final ThreadLocal<FailedRecord> failures = new ThreadLocal<>();

	private final BiConsumer<ConsumerRecord<K, V>, Exception> recoverer;

	private final int maxFailures;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value #DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor() {
		this(null, DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value #DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor(BiConsumer<ConsumerRecord<K, V>, Exception> recoverer) {
		this(recoverer, DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * maxFailures have occurred for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param maxFailures the maxFailures.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor(@Nullable BiConsumer<ConsumerRecord<K, V>, Exception> recoverer, int maxFailures) {
		if (recoverer == null) {
			this.recoverer = (r, t) -> logger.error("Max failures (" + maxFailures + ") reached for: " + r, t);
		}
		else {
			this.recoverer = recoverer;
		}
		this.maxFailures = maxFailures;
	}

	@Override
	public void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer, Exception exception,
			boolean recoverable) {
		Map<TopicPartition, Long> partitions = new HashMap<>();
		AtomicBoolean first = new AtomicBoolean(true);
		records.forEach(record ->  {
			if (!recoverable || !first.get() || !skip(record, exception)) {
				partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), offset -> record.offset());
			}
			first.set(false);
		});
		partitions.forEach((topicPartition, offset) -> {
			try {
				consumer.seek(topicPartition, offset);
			}
			catch (Exception e) {
				logger.error("Failed to seek " + topicPartition + " to " + offset);
			}
		});
	}

	private boolean skip(ConsumerRecord<K, V> record, Exception exception) {
		FailedRecord failedRecord = failures.get();
		if (failedRecord == null || !failedRecord.getTopic().equals(record.topic())
				|| failedRecord.getPartition() != record.partition() || failedRecord.getOffset() != record.offset()) {
			failures.set(new FailedRecord(record.topic(), record.partition(), record.offset()));
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

	@Override
	public void clearThreadState() {
		failures.remove();
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
