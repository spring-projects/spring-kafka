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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.support.SendResult;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * A listenable future for requests/replies.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public class RequestReplyFuture<K, V, R> extends SettableListenableFuture<ConsumerRecord<K, R>> {

	private volatile ListenableFuture<SendResult<K, V>> sendFuture;

	private CompletableFuture<SendResult<K, V>> completableSendFuture;

	private Completable completable;

	protected void setSendFuture(ListenableFuture<SendResult<K, V>> sendFuture) {
		this.sendFuture = sendFuture;
	}

	/**
	 * Return the send future.
	 * @return the send future.
	 */
	public ListenableFuture<SendResult<K, V>> getSendFuture() {
		return this.sendFuture;
	}

	/**
	 * Return a {@link CompletableFuture} representation of this instance.
	 * @return the {@link CompletableFuture}.
	 * @since 2.9
	 */
	public synchronized Completable asCompletable() {
		if (this.completable == null) {
			this.completable = new Completable(this);
			addCallback(this.completable::complete, this.completable::completeExceptionally);
		}
		return this.completable;
	}

	/**
	 * A {@link CompletableFuture} version.
	 */
	public class Completable extends CompletableFuture<ConsumerRecord<K, R>> {

		private final Future<ConsumerRecord<K, R>> delegate;

		Completable(Future<ConsumerRecord<K, R>> delegate) {
			Assert.notNull(delegate, "Delegate must not be null");
			this.delegate = delegate;
		}

		/**
		 * Return the send future as a {@link CompletableFuture}.
		 * @return the send future.
		 * @since 2.9
		 */
		public synchronized CompletableFuture<SendResult<K, V>> getSendFuture() {
			if (RequestReplyFuture.this.completableSendFuture == null) {
				RequestReplyFuture.this.completableSendFuture = RequestReplyFuture.this.sendFuture.completable();
			}
			return RequestReplyFuture.this.completableSendFuture;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			boolean result = this.delegate.cancel(mayInterruptIfRunning);
			super.cancel(mayInterruptIfRunning);
			return result;
		}

	}

}
