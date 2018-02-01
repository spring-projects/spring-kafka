/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.requestreply;

import org.apache.kafka.clients.producer.ProducerRecord;

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
	 * Send a request and receive a reply.
	 * @param record the record to send.
	 * @return a RequestReplyFuture.
	 */
	RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record);

}
