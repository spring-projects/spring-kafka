/*
 * Copyright 2019 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;

/**
 * An interceptor for {@link ConsumerRecord} invoked by the listener
 * container before invoking the listener.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.2.7
 *
 */
@FunctionalInterface
public interface RecordInterceptor<K, V> {

	/**
	 * Perform some action on the record or return a different one.
	 * If null is returned the record will be skipped.
	 * @param record the record.
	 * @return the record or null.
	 */
	@Nullable
	ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record);

}
