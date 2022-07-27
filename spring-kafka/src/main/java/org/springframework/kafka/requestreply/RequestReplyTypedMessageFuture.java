/*
 * Copyright 2021-2022 the original author or authors.
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.util.concurrent.ListenableFuture;

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

	private static final String UNCHECKED = "unchecked";

	private Completable completable;

	RequestReplyTypedMessageFuture(ListenableFuture<SendResult<K, V>> sendFuture) {
		super(sendFuture);
	}

	@SuppressWarnings(UNCHECKED)
	@Override
	public Message<P> get() throws InterruptedException, ExecutionException {
		return (Message<P>) super.get();
	}

	@SuppressWarnings(UNCHECKED)
	@Override
	public Message<P> get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {

		return (Message<P>) super.get(timeout, unit);
	}

	@Override
	@SuppressWarnings(UNCHECKED)
	public synchronized Completable asCompletable() {
		if (this.completable == null) {
			this.completable = new Completable(this, this);
			addCallback(this.completable::complete, this.completable::completeExceptionally);
		}
		return this.completable;
	}

	/**
	 * A {@link CompletableFuture} version.
	 * @since 2.9
	 */
	public class Completable extends RequestReplyMessageFuture<K, V>.Completable {

		Completable(RequestReplyMessageFuture<K, V> requestReplyMessageFuture, Future<Message<?>> delegate) { // NOSONAR
			requestReplyMessageFuture.super(delegate);
		}

		@SuppressWarnings(UNCHECKED)
		@Override
		public Message<P> get() throws InterruptedException, ExecutionException {
			return (Message<P>) super.get();
		}

		@SuppressWarnings(UNCHECKED)
		@Override
		public Message<P> get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {

			return (Message<P>) super.get(timeout, unit);
		}

	}

}
