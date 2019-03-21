/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.core;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * If a {@code ConsumerFactory} implements this interface, it supports customizing the
 * {@code client.id} by adding a suffix, if a {@code client.id} property is provided.
 *
 * @deprecated - in 2.0, this method will move to {@link ConsumerFactory}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 1.0.6
 *
 */
@Deprecated
public interface ClientIdSuffixAware<K, V> {

	/**
	 * Create a consumer, appending the suffix to the {@code client.id} property,
	 * if present.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 */
	Consumer<K, V> createConsumer(String clientIdSuffix);

}
