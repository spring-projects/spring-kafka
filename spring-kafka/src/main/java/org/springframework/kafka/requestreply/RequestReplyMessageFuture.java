/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.requestreply;

import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;

/**
 * A listenable future for {@link Message} replies.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
public class RequestReplyMessageFuture<K, V> extends CompletableFuture<Message<?>> {

	private final @Nullable CompletableFuture<SendResult<K, V>> sendFuture; // NOSONAR

	RequestReplyMessageFuture(@Nullable CompletableFuture<SendResult<K, V>> sendFuture) {
		this.sendFuture = sendFuture;
	}

	/**
	 * Return the send future.
	 * @return the send future.
	 */
	public @Nullable CompletableFuture<SendResult<K, V>> getSendFuture() {
		return this.sendFuture;
	}

}
