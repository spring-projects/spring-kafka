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

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.ParallelConsumerCallback;

import io.confluent.parallelconsumer.JStreamParallelStreamProcessor;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable.DrainingMode;

/**
 * This class is for aggregating all related with ParallelConsumer.
 * @author ...
 * @since 3.2.0
 */


public class ParallelConsumerContext<K,V> {

	public static final String DEFAULT_BEAN_NAME = "parallelConsumerContext";
	private final ParallelConsumerConfig parallelConsumerConfig;
	private final ParallelConsumerCallback<K, V> parallelConsumerCallback;

	public ParallelConsumerContext(ParallelConsumerConfig parallelConsumerConfig,
								   ParallelConsumerCallback<K, V> callback) {
		this.parallelConsumerConfig = parallelConsumerConfig;
		this.parallelConsumerCallback = callback;
	}

	public ParallelConsumerCallback<K, V> parallelConsumerCallback() {
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

	public void stop(ParallelStreamProcessor<K, V> parallelStreamProcessor) {
		parallelStreamProcessor.close();

	}

}
