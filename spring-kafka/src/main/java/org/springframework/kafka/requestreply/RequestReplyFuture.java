/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.requestreply;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.SendResult;

/**
 * A {@link CompletableFuture} for requests/replies.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public class RequestReplyFuture<K, V, R> extends CompletableFuture<ConsumerRecord<K, R>> {

	private volatile @Nullable CompletableFuture<SendResult<K, V>> sendFuture;

	protected void setSendFuture(CompletableFuture<SendResult<K, V>> sendFuture) {
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
