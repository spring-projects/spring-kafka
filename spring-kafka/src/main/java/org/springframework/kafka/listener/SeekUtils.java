/*
 * Copyright 2018-2020 the original author or authors.
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.util.ObjectUtils;
import org.springframework.util.backoff.FixedBackOff;

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

	/**
	 * The default back off - a {@link FixedBackOff} with 0 interval and
	 * {@link #DEFAULT_MAX_FAILURES} - 1 retry attempts.
	 */
	public static final FixedBackOff DEFAULT_BACK_OFF = new FixedBackOff(0, DEFAULT_MAX_FAILURES - 1);

	private static final LoggingCommitCallback LOGGING_COMMIT_CALLBACK = new LoggingCommitCallback();

	private SeekUtils() {
	}

	/**
	 * Seek records to earliest position, optionally skipping the first.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @param exception the exception
	 * @param recoverable true if skipping the first record is allowed.
	 * @param skipper function to determine whether or not to skip seeking the first.
	 * @param logger a {@link LogAccessor} for seek errors.
	 * @return true if the failed record was skipped.
	 */
	public static boolean doSeeks(List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, Exception exception,
			boolean recoverable, BiPredicate<ConsumerRecord<?, ?>, Exception> skipper, LogAccessor logger) {

		Map<TopicPartition, Long> partitions = new LinkedHashMap<>();
		AtomicBoolean first = new AtomicBoolean(true);
		AtomicBoolean skipped = new AtomicBoolean();
		records.forEach(record -> {
			if (recoverable && first.get()) {
				try {
					boolean test = skipper.test(record, exception);
					skipped.set(test);
				}
				catch (Exception ex) {
					logger.error(ex, "Failed to determine if this record (" + record
							+ ") should be recovererd, including in seeks");
					skipped.set(false);
				}
				if (skipped.get()) {
					logger.debug(() -> "Skipping seek of: " + record);
				}
			}
			if (!recoverable || !first.get() || !skipped.get()) {
				partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
						offset -> record.offset());
			}
			first.set(false);
		});
		seekPartitions(consumer, partitions, logger);
		return skipped.get();
	}

	/**
	 * Perform seek operations on each partition.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 * @param logger the logger.
	 * @since 2.5
	 */
	public static void seekPartitions(Consumer<?, ?> consumer, Map<TopicPartition, Long> partitions,
			LogAccessor logger) {

		partitions.forEach((topicPartition, offset) -> {
			try {
				logger.trace(() -> "Seeking: " + topicPartition + " to: " + offset);
				consumer.seek(topicPartition, offset);
			}
			catch (Exception e) {
				logger.error(e, () -> "Failed to seek " + topicPartition + " to " + offset);
			}
		});
	}

	/**
	 * Seek the remaining records, optionally recovering the first.
	 * @param thrownException the exception.
	 * @param records the remaining records.
	 * @param consumer the consumer.
	 * @param container the container.
	 * @param commitRecovered true to commit the recovererd record offset.
	 * @param skipPredicate the skip predicate.
	 * @param logger the logger.
	 * @since 2.5
	 */
	public static void seekOrRecover(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container, boolean commitRecovered,
			BiPredicate<ConsumerRecord<?, ?>, Exception> skipPredicate, LogAccessor logger) {

		if (ObjectUtils.isEmpty(records)) {
			if (thrownException instanceof SerializationException) {
				throw new IllegalStateException("This error handler cannot process 'SerializationException's directly; "
						+ "please consider configuring an 'ErrorHandlingDeserializer' in the value and/or key "
						+ "deserializer", thrownException);
			}
			else {
				throw new IllegalStateException("This error handler cannot process '"
						+ thrownException.getClass().getName()
						+ "'s; no record information is available", thrownException);
			}
		}

		if (!doSeeks(records, consumer, thrownException, true, skipPredicate, logger)) {
			throw new KafkaException("Seek to current after exception", thrownException);
		}
		if (commitRecovered) {
			if (container.getContainerProperties().getAckMode().equals(AckMode.MANUAL_IMMEDIATE)) {
				ConsumerRecord<?, ?> record = records.get(0);
				Map<TopicPartition, OffsetAndMetadata> offsetToCommit = Collections.singletonMap(
						new TopicPartition(record.topic(), record.partition()),
						new OffsetAndMetadata(record.offset() + 1));
				if (container.getContainerProperties().isSyncCommits()) {
					consumer.commitSync(offsetToCommit, container.getContainerProperties().getSyncCommitTimeout());
				}
				else {
					OffsetCommitCallback commitCallback = container.getContainerProperties().getCommitCallback();
					if (commitCallback == null) {
						commitCallback = LOGGING_COMMIT_CALLBACK;
					}
					consumer.commitAsync(offsetToCommit, commitCallback);
				}
			}
			else {
				logger.warn(() -> "'commitRecovered' ignored, container AckMode must be MANUAL_IMMEDIATE, not "
						+ container.getContainerProperties().getAckMode());
			}
		}
	}

}
