/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.requestreply;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;

/**
 * A listenable future for {@link Message} replies with a specific payload type.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <P> the reply payload type.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
public class RequestReplyTypedMessageFuture<K, V, P> extends RequestReplyMessageFuture<K, V> {

	RequestReplyTypedMessageFuture(@Nullable CompletableFuture<SendResult<K, V>> sendFuture) {
		super(sendFuture);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message<P> get() throws InterruptedException, ExecutionException {
		return (Message<P>) super.get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message<P> get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {

		return (Message<P>) super.get(timeout, unit);
	}

}
