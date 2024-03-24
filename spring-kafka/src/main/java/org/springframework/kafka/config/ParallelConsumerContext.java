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
import org.springframework.kafka.core.ParallelConsumerCallback;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;

/**
 * This class is for aggregating all related with ParallelConsumer.
 * @author ...
 * @since 3.2.0
 */


public class ParallelConsumerContext<K,V> {

	public static final String DEFAULT_BEAN_NAME = "parallelConsumerContext";
	private final ParallelConsumerConfig parallelConsumerConfig;
	private final ParallelConsumerCallback<K,V> parallelConsumerCallback;
	private ParallelStreamProcessor<K, V> processor;

	public ParallelConsumerContext(ParallelConsumerCallback<K, V> callback) {
		this.parallelConsumerConfig = new ParallelConsumerConfig();
		this.parallelConsumerCallback = callback;
	}


	public ParallelConsumerCallback<K,V> parallelConsumerCallback() {
		return this.parallelConsumerCallback;
	}


	public ParallelStreamProcessor<K, V> createConsumer(Consumer<K,V> consumer) {
		final ParallelConsumerOptions<K, V> options = parallelConsumerConfig.toConsumerOptions(consumer);
		this.processor = ParallelStreamProcessor.createEosStreamProcessor(options);
		return this.processor;
	}

	public void stopParallelConsumer() {
		this.processor.close();
	}

}