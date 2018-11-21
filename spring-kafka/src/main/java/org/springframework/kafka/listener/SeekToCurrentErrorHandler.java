/*
 * Copyright 2017 the original author or authors.
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

import java.util.List;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.lang.Nullable;

/**
 * An error handler that seeks to the current offset for each topic in the remaining
 * records. Used to rewind partitions after a message failure so that it can be
 * replayed.
 *
 * @author Gary Russell
 * @since 2.0.1
 *
 */
public class SeekToCurrentErrorHandler implements ContainerAwareErrorHandler {

	private static final Log logger = LogFactory.getLog(SeekToCurrentErrorHandler.class); // NOSONAR

	private final FailedRecordTracker failureTracker;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler() {
		this(null, SeekUtils.DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * 'maxFailures' have occurred for a topic/partition/offset.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @since 2.2.1
	 */
	public SeekToCurrentErrorHandler(int maxFailures) {
		this(null, maxFailures);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * maxFailures have occurred for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, int maxFailures) {
		this.failureTracker = new FailedRecordTracker(recoverer, maxFailures, logger);
	}

	@Override
	public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {
		if (!SeekUtils.doSeeks(records, consumer, thrownException, true, this.failureTracker::skip, logger)) {
			throw new KafkaException("Seek to current after exception", thrownException);
		}
	}

	@Override
	public void clearThreadState() {
		this.failureTracker.clearThreadState();
	}

}
