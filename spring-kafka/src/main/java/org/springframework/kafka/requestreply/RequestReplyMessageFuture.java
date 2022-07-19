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

	private final CompletableFuture<SendResult<K, V>> sendFuture; // NOSONAR

	RequestReplyMessageFuture(CompletableFuture<SendResult<K, V>> sendFuture) {
		this.sendFuture = sendFuture;
	}

	/**
	 * Return the send future.
	 * @return the send future.
	 */
	public CompletableFuture<SendResult<K, V>> getSendFuture() {
		return this.sendFuture;
	}

}
