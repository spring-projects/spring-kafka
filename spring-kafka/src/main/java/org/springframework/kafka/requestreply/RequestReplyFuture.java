/*
 * Copyright 2018-present the original author or authors.
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
