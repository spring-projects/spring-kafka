/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.lang.Nullable;

/**
 * An interceptor for batches of records.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
@FunctionalInterface
public interface BatchInterceptor<K, V> {

	/**
	 * Perform some action on the records or return a different one. If null is returned
	 * the records will be skipped. Invoked before the listener.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @return the records or null.
	 */
	@Nullable
	ConsumerRecords<K, V> intercept(ConsumerRecords<K, V> records, Consumer<K, V> consumer);

	/**
	 * Called after the listener exits normally.
	 * @param records the records.
	 * @param consumer the consumer.
	 */
	default void success(ConsumerRecords<K, V> records, Consumer<K, V> consumer) {
	}

	/**
	 * Called after the listener throws an exception.
	 * @param records the records.
	 * @param exception the exception.
	 * @param consumer the consumer.
	 */
	default void failure(ConsumerRecords<K, V> records, Exception exception, Consumer<K, V> consumer) {
	}

}
