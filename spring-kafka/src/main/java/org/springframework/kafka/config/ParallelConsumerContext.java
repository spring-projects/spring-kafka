/*
 * Copyright 2014-2024 the original author or authors.
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

package org.springframework.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.parallelconsumer.ParallelConsumerRootInterface;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;

/**
 * This class is for collecting all related with ParallelConsumer.
 *
 * @author Sanghyeok An
 *
 * @since 3.3
 */


public class ParallelConsumerContext<K,V> {

	public static final String DEFAULT_BEAN_NAME = "parallelConsumerContext";
	private final ParallelConsumerConfig parallelConsumerConfig;
	private final ParallelConsumerRootInterface<K, V> parallelConsumerCallback;

	public ParallelConsumerContext(ParallelConsumerConfig<K, V> parallelConsumerConfig,
								   ParallelConsumerRootInterface<K, V> callback) {
		this.parallelConsumerConfig = parallelConsumerConfig;
		this.parallelConsumerCallback = callback;
	}

	public ParallelConsumerRootInterface<K, V> parallelConsumerCallback() {
		return this.parallelConsumerCallback;
	}

	public ParallelConsumerOptions<K, V> getParallelConsumerOptions(Consumer<K, V> consumer) {
		final ParallelConsumerOptionsBuilder<K, V> builder = ParallelConsumerOptions.builder();
		return parallelConsumerConfig.toConsumerOptions(builder, consumer);
	}

	public ParallelConsumerOptions<K, V> getParallelConsumerOptions(Consumer<K, V> consumer, Producer<K, V> producer) {
		final ParallelConsumerOptionsBuilder<K, V> builder = ParallelConsumerOptions.builder();
		return parallelConsumerConfig.toConsumerOptions(builder, consumer, producer);
	}

}
