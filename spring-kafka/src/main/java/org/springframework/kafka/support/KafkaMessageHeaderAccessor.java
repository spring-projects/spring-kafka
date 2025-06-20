/*
 * Copyright 2023-present the original author or authors.
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

import java.nio.ByteBuffer;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;

/**
 * A header accessor to provide convenient access to certain headers in a
 * type specific manner.
 *
 * @author Gary Russell
 * @since 3.0.10
 *
 */
public class KafkaMessageHeaderAccessor extends MessageHeaderAccessor {

	/**
	 * Construct an instance for the provided message.
	 * @param message the message.
	 */
	public KafkaMessageHeaderAccessor(Message<?> message) {
		super(message);
	}

	/**
	 * Access the header value when the blocking delivery attempt header is present.
	 * @return The decoded header value if the header is present.
	 * @throws IllegalStateException if the header is not present.
	 * @see org.springframework.kafka.listener.ContainerProperties#setDeliveryAttemptHeader(boolean)
	 */
	public int getBlockingRetryDeliveryAttempt() {
		Assert.state(getHeader(KafkaHeaders.DELIVERY_ATTEMPT) != null,
				"Blocking delivery attempt header not present, "
				+ "see ContainerProperties.setDeliveryAttemptHeader() to enable");
		Integer deliveryAttempts = getHeader(KafkaHeaders.DELIVERY_ATTEMPT, Integer.class);
		return deliveryAttempts == null ? 0 : deliveryAttempts;
	}

	/**
	 * When using non-blocking retries, get the delivery attempt header value as an int.
	 * @return 1 if there is no header present; the decoded header value otherwise.
	 */
	public int getNonBlockingRetryDeliveryAttempt() {
		return fromBytes(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
	}

	private int fromBytes(String headerName) {
		byte[] header = getHeader(headerName, byte[].class);
		return header == null ? 1 : ByteBuffer.wrap(header).getInt();
	}

	/**
	 * Get a header value with a specific type.
	 * @param <T> the type.
	 * @param key the header name.
	 * @param type the type's {@link Class}.
	 * @return the value, if present.
	 * @throws IllegalArgumentException if the type is not correct.
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T getHeader(String key, Class<T> type) {
		Object value = getHeader(key);
		if (value == null) {
			return null;
		}
		if (!type.isAssignableFrom(value.getClass())) {
			throw new IllegalArgumentException("Incorrect type specified for header '" + key + "'. Expected [" + type
					+ "] but actual type is [" + value.getClass() + "]");
		}
		return (T) value;
	}

	@Override
	protected MessageHeaderAccessor createAccessor(Message<?> message) {
		return wrap(message);
	}

	/**
	 * Create an instance from the payload and headers of the given Message.
	 * @param message the message.
	 * @return the accessor.
	 */
	public static KafkaMessageHeaderAccessor wrap(Message<?> message) {
		return new KafkaMessageHeaderAccessor(message);
	}

}
