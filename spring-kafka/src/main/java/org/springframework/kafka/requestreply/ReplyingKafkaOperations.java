/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.requestreply;

import java.time.Duration;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

/**
 * Request/reply operations.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public interface ReplyingKafkaOperations<K, V, R> {

	/**
	 * Wait until partitions are assigned, e.g. when {@code auto.offset.reset=latest}.
	 * When using manual assignment, the duration must be greater than the container's
	 * {@code pollTimeout} property.
	 * @param duration how long to wait.
	 * @return true if the partitions have been assigned.
	 * @throws InterruptedException if the thread is interrupted while waiting.
	 * @since 2.8.8
	 */
	default boolean waitForAssignment(Duration duration) throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Send a request message and receive a reply message with the default timeout.
	 * @param message the message to send.
	 * @return a RequestReplyMessageFuture.
	 * @since 2.7
	 *
	 */
	default RequestReplyMessageFuture<K, V> sendAndReceive(Message<?> message) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Send a request message and receive a reply message.
	 * @param message the message to send.
	 * @param replyTimeout the reply timeout; if null, the default will be used.
	 * @return a RequestReplyMessageFuture.
	 * @since 2.7
	 */
	default RequestReplyMessageFuture<K, V> sendAndReceive(Message<?> message, @Nullable Duration replyTimeout) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Send a request message and receive a reply message.
	 * @param message the message to send.
	 * @param returnType a hint to the message converter for the reply payload type.
	 * @param <P> the reply payload type.
	 * @return a RequestReplyMessageFuture.
	 * @since 2.7
	 */
	default <P> RequestReplyTypedMessageFuture<K, V, P> sendAndReceive(Message<?> message,
			ParameterizedTypeReference<P> returnType) {

		throw new UnsupportedOperationException();
	}

	/**
	 * Send a request message and receive a reply message.
	 * @param message the message to send.
	 * @param replyTimeout the reply timeout; if null, the default will be used.
	 * @param returnType a hint to the message converter for the reply payload type.
	 * @param <P> the reply payload type.
	 * @return a RequestReplyMessageFuture.
	 * @since 2.7
	 */
	default <P> RequestReplyTypedMessageFuture<K, V, P> sendAndReceive(Message<?> message, Duration replyTimeout,
			ParameterizedTypeReference<P> returnType) {

		throw new UnsupportedOperationException();
	}

	/**
	 * Send a request and receive a reply with the default timeout.
	 * @param record the record to send.
	 * @return a RequestReplyFuture.
	 */
	RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record);

	/**
	 * Send a request and receive a reply.
	 * @param record the record to send.
	 * @param replyTimeout the reply timeout; if null, the default will be used.
	 * @return a RequestReplyFuture.
	 * @since 2.3
	 */
	RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record, Duration replyTimeout);

}
