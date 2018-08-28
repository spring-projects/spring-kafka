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

package org.springframework.kafka.support;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;

import org.apache.commons.logging.Log;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Seek utilities.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public final class SeekUtils {

	/**
	 * The number of times a topic/partition/offset can fail before being rejected.
	 */
	public static final int DEFAULT_MAX_FAILURES = 10;

	private SeekUtils() {
		super();
	}

	/**
	 * Seek records to earliest position, optionally skipping the first.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @param exception the exception
	 * @param recoverable true if skipping the first record is allowed.
	 * @param skipper function to determine whether or not to skip seeking the first.
	 * @param logger a {@link Log} for seek errors.
	 * @return true if the failed record was skipped.
	 */
	public static boolean doSeeks(List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, Exception exception,
			boolean recoverable, BiPredicate<ConsumerRecord<?, ?>, Exception> skipper, Log logger) {
		Map<TopicPartition, Long> partitions = new LinkedHashMap<>();
		AtomicBoolean first = new AtomicBoolean(true);
		AtomicBoolean skipped = new AtomicBoolean();
		records.forEach(record ->  {
			if (recoverable && first.get()) {
				skipped.set(skipper.test(record, exception));
				if (skipped.get() && logger.isDebugEnabled()) {
					logger.debug("Skipping seek of: " + record);
				}
			}
			if (!recoverable || !first.get() || !skipped.get()) {
				partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
						offset -> record.offset());
			}
			first.set(false);
		});
		boolean tracing = logger.isTraceEnabled();
		partitions.forEach((topicPartition, offset) -> {
			try {
				if (tracing) {
					logger.trace("Seeking: " + topicPartition + " to: " + offset);
				}
				consumer.seek(topicPartition, offset);
			}
			catch (Exception e) {
				logger.error("Failed to seek " + topicPartition + " to " + offset, e);
			}
		});
		return skipped.get();
	}

}
