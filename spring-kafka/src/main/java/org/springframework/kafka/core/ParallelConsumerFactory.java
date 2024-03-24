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

package org.springframework.kafka.core;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ParallelConsumerContext;

import io.confluent.parallelconsumer.ParallelStreamProcessor;

/**
 * ParallelConsumerFactory will be started and closed by Spring LifeCycle.
 * This class is quite simple, because ParallelConsumer requires delegating the situation to itself.
 * @author ...
 * @since 3.2.0
 */

public class ParallelConsumerFactory<K, V> implements SmartLifecycle {

	public static final String DEFAULT_BEAN_NAME = "parallelConsumerFactory";
	private final ParallelConsumerContext<K, V> parallelConsumerContext;
	private final DefaultKafkaConsumerFactory<K, V> defaultKafkaConsumerFactory;
	private boolean running;

	public ParallelConsumerFactory(ParallelConsumerContext<K, V> parallelConsumerContext,
								   DefaultKafkaConsumerFactory<K, V> defaultKafkaConsumerFactory) {
		this.parallelConsumerContext = parallelConsumerContext;
		this.defaultKafkaConsumerFactory = defaultKafkaConsumerFactory;
	}


	@Override
	public void start() {
		final Consumer<K, V> consumer = defaultKafkaConsumerFactory.createConsumer();
		final ParallelStreamProcessor<K, V> parallelConsumer = parallelConsumerContext.createConsumer(consumer);
		parallelConsumer.subscribe(parallelConsumerContext.parallelConsumerCallback().getTopics());
		parallelConsumer.poll(recordContexts -> parallelConsumerContext.parallelConsumerCallback().accept(recordContexts));
		this.running = true;
	}

	@Override
	public void stop() {
		this.parallelConsumerContext.stopParallelConsumer();
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}
}