/*
 * Copyright 2017-2022 the original author or authors.
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
import java.io.ObjectStreamClass;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
 * @author Francois Rosiere
 * @since 2.0
 *
 */
public final class ListenerUtils {

	private ListenerUtils() {
	}

	private static final int DEFAULT_SLEEP_INTERVAL = 100;

	private static final int SMALL_SLEEP_INTERVAL = 10;

	private static final long SMALL_INTERVAL_THRESHOLD = 500;

	/**
	 * Determine the type of the listener.
	 * @param listener the listener.
	 * @return the {@link ListenerType}.
	 */
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
			byte[] value = header.value();
			DeserializationException exception = byteArrayToDeserializationException(logger, value);
			if (exception != null) {
				Headers headers = new RecordHeaders(record.headers().toArray());
				headers.remove(headerName);
				exception.setHeaders(headers);
			}
			return exception;
		}
		return null;
	}

	/**
	 * Convert a byte array containing a serialized {@link DeserializationException} to the
	 * {@link DeserializationException}.
	 * @param logger a log accessor to log errors.
	 * @param value the bytes.
	 * @return the exception or null if deserialization fails.
	 * @since 2.8.1
	 */
	@Nullable
	public static DeserializationException byteArrayToDeserializationException(LogAccessor logger, byte[] value) {
		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value)) {

				boolean first = true;

				@Override
				protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
					if (this.first) {
						this.first = false;
						Assert.state(desc.getName().equals(DeserializationException.class.getName()),
								"Header does not contain a DeserializationException");
					}
					return super.resolveClass(desc);
				}


			};
			return (DeserializationException) ois.readObject();
		}
		catch (IOException | ClassNotFoundException | ClassCastException e) {
			logger.error(e, "Failed to deserialize a deserialization exception");
			return null;
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
	 * @param container the container or parent container.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 2.7
	 */
	public static void unrecoverableBackOff(BackOff backOff, ThreadLocal<BackOffExecution> executions,
			ThreadLocal<Long> lastIntervals, MessageListenerContainer container) throws InterruptedException {

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
			stoppableSleep(container, interval);
		}
	}

	/**
	 * Sleep for the desired timeout, as long as the container continues to run.
	 * @param container the container.
	 * @param interval the timeout.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 2.7
	 */
	public static void stoppableSleep(MessageListenerContainer container, long interval) throws InterruptedException {
		long timeout = System.currentTimeMillis() + interval;
		long sleepInterval = interval > SMALL_INTERVAL_THRESHOLD ? DEFAULT_SLEEP_INTERVAL : SMALL_SLEEP_INTERVAL;
		do {
			Thread.sleep(sleepInterval);
			if (!container.isRunning()) {
				break;
			}
		}
		while (System.currentTimeMillis() < timeout);
	}

	/**
	 * Create a new {@link  OffsetAndMetadata} using the given container and offset.
	 * @param container a container.
	 * @param offset an offset.
	 * @return an offset and metadata.
	 * @since 2.8.6
	 */
	public static OffsetAndMetadata createOffsetAndMetadata(MessageListenerContainer container,
															long offset) {
		final OffsetAndMetadataProvider metadataProvider = container.getContainerProperties()
				.getOffsetAndMetadataProvider();
		if (metadataProvider != null) {
			return metadataProvider.provide(new DefaultListenerMetadata(container), offset);
		}
		return new OffsetAndMetadata(offset);
	}
}

