/*
 * Copyright 2019-2022 the original author or authors.
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
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;

/**
 * An interceptor for {@link ConsumerRecord} invoked by the listener
 * container before and after invoking the listener.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.2.7
 *
 */
@FunctionalInterface
public interface RecordInterceptor<K, V> extends ThreadStateProcessor {

	/**
	 * Perform some action on the record or return a different one. If null is returned
	 * the record will be skipped. Invoked before the listener. IMPORTANT; if this method
	 * returns a different record, the topic, partition and offset must not be changed
	 * to avoid undesirable side-effects.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @return the record or null.
	 * @since 2.7
	 */
	@Nullable
	ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer);

	/**
	 * Called after the listener exits normally.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @since 2.7
	 */
	default void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
	}

	/**
	 * Called after the listener throws an exception.
	 * @param record the record.
	 * @param exception the exception.
	 * @param consumer the consumer.
	 * @since 2.7
	 */
	default void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
	}

	/**
	 * Called when processing the record is complete either
	 * {@link #success(ConsumerRecord, Consumer)} or
	 * {@link #failure(ConsumerRecord, Exception, Consumer)}.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @since 2.8
	 */
	default void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
	}

}
