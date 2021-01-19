/*
 * Copyright 2017-2021 the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Listener utilities.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public final class ListenerUtils {

	private ListenerUtils() {
	}

	private static final ThreadLocal<Boolean> LOG_METADATA_ONLY = new ThreadLocal<>();

	public static ListenerType determineListenerType(Object listener) {
		Assert.notNull(listener, "Listener cannot be null");
		ListenerType listenerType;
		if (listener instanceof AcknowledgingConsumerAwareMessageListener
				|| listener instanceof BatchAcknowledgingConsumerAwareMessageListener) {
			listenerType = ListenerType.ACKNOWLEDGING_CONSUMER_AWARE;
		}
		else if (listener instanceof ConsumerAwareMessageListener
				|| listener instanceof BatchConsumerAwareMessageListener) {
			listenerType = ListenerType.CONSUMER_AWARE;
		}
		else if (listener instanceof AcknowledgingMessageListener
				|| listener instanceof BatchAcknowledgingMessageListener) {
			listenerType = ListenerType.ACKNOWLEDGING;
		}
		else if (listener instanceof GenericMessageListener) {
			listenerType = ListenerType.SIMPLE;
		}
		else {
			throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
		}
		return listenerType;
	}

	/**
	 * Extract a {@link DeserializationException} from the supplied header name, if
	 * present.
	 * @param record the consumer record.
	 * @param headerName the header name.
	 * @param logger the logger for logging errors.
	 * @return the exception or null.
	 * @since 2.3
	 */
	@Nullable
	public static DeserializationException getExceptionFromHeader(final ConsumerRecord<?, ?> record,
			String headerName, LogAccessor logger) {

		Header header = record.headers().lastHeader(headerName);
		if (header != null) {
			try {
				DeserializationException ex = (DeserializationException) new ObjectInputStream(
						new ByteArrayInputStream(header.value())).readObject();
				Headers headers = new RecordHeaders(record.headers().toArray());
				headers.remove(headerName);
				ex.setHeaders(headers);
				return ex;
			}
			catch (IOException | ClassNotFoundException | ClassCastException e) {
				logger.error(e, "Failed to deserialize a deserialization exception");
			}
		}
		return null;
	}

	/**
	 * Set to true to only log record metadata.
	 * @param onlyMeta true to only log record metadata.
	 * @since 2.2.14
	 * @see #recordToString(ConsumerRecord)
	 */
	public static void setLogOnlyMetadata(boolean onlyMeta) {
		LOG_METADATA_ONLY.set(onlyMeta);
	}

	/**
	 * Return the {@link ConsumerRecord} as a String; either {@code toString()} or
	 * {@code topic-partition@offset}.
	 * @param record the record.
	 * @return the rendered record.
	 * @since 2.2.14
	 * @see #setLogOnlyMetadata(boolean)
	 */
	public static String recordToString(ConsumerRecord<?, ?> record) {
		if (Boolean.TRUE.equals(LOG_METADATA_ONLY.get())) {
			return record.topic() + "-" + record.partition() + "@" + record.offset();
		}
		else {
			return record.toString();
		}
	}

	/**
	 * Return the {@link ConsumerRecord} as a String; either {@code toString()} or
	 * {@code topic-partition@offset}.
	 * @param record the record.
	 * @param meta true to log just the metadata.
	 * @return the rendered record.
	 * @since 2.5.4
	 */
	public static String recordToString(ConsumerRecord<?, ?> record, boolean meta) {
		if (meta) {
			return record.topic() + "-" + record.partition() + "@" + record.offset();
		}
		else {
			return record.toString();
		}
	}

	/**
	 * Sleep according to the {@link BackOff}; when the {@link BackOffExecution} returns
	 * {@link BackOffExecution#STOP} sleep for the previous backOff.
	 * @param backOff the {@link BackOff} to create a new {@link BackOffExecution}.
	 * @param executions a thread local containing the {@link BackOffExecution} for this
	 * thread.
	 * @param lastIntervals a thread local containing the previous {@link BackOff}
	 * interval for this thread.
	 * @since 2.3.12
	 */
	public static void unrecoverableBackOff(BackOff backOff, ThreadLocal<BackOffExecution> executions,
			ThreadLocal<Long> lastIntervals) {

		BackOffExecution backOffExecution = executions.get();
		if (backOffExecution == null) {
			backOffExecution = backOff.start();
			executions.set(backOffExecution);
		}
		Long interval = backOffExecution.nextBackOff();
		if (interval == BackOffExecution.STOP) {
			interval = lastIntervals.get();
			if (interval == null) {
				interval = Long.valueOf(0);
			}
		}
		lastIntervals.set(interval);
		if (interval > 0) {
			try {
				Thread.sleep(interval);
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

}

