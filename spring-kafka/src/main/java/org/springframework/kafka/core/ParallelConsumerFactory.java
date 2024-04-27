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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ParallelConsumerContext;
import org.springframework.kafka.core.parallelconsumer.ParallelConsumerCallback;
import org.springframework.kafka.core.parallelconsumer.PollAndProduce;
import org.springframework.kafka.core.parallelconsumer.PollAndProduceMany;
import org.springframework.kafka.core.parallelconsumer.PollAndProduceManyResult;
import org.springframework.kafka.core.parallelconsumer.PollAndProduceResult;
import org.springframework.kafka.core.parallelconsumer.Poll;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable.DrainingMode;

/**
 * ParallelConsumerFactory will be started and closed by Spring LifeCycle.
 * This class is quite simple, because ParallelConsumer requires delegating the situation to itself.
 * @author Sanghyeok An
 * @since 3.3
 */

public class ParallelConsumerFactory<K, V> implements SmartLifecycle {

	public static final String DEFAULT_BEAN_NAME = "parallelConsumerFactory";

	private final DefaultKafkaConsumerFactory<K, V> defaultKafkaConsumerFactory;
	private final DefaultKafkaProducerFactory<K, V> defaultKafkaProducerFactory;
	private final ParallelConsumerContext<K, V> parallelConsumerContext;
	private final ParallelStreamProcessor<K, V> parallelConsumer;
	private final ParallelConsumerOptions<K, V> parallelConsumerOptions;
	private boolean running;

	public ParallelConsumerFactory(ParallelConsumerContext<K, V> parallelConsumerContext,
								   DefaultKafkaConsumerFactory<K, V> defaultKafkaConsumerFactory,
								   DefaultKafkaProducerFactory<K, V> defaultKafkaProducerFactory) {
		this.parallelConsumerContext = parallelConsumerContext;
		this.defaultKafkaConsumerFactory = defaultKafkaConsumerFactory;
		this.defaultKafkaProducerFactory = defaultKafkaProducerFactory;

		final Consumer<K, V> kafkaConsumer = defaultKafkaConsumerFactory.createConsumer();
		final Producer<K, V> kafkaProducer = defaultKafkaProducerFactory.createProducer();
		this.parallelConsumerOptions = parallelConsumerOptions(kafkaConsumer, kafkaProducer);
		this.parallelConsumer = ParallelStreamProcessor.createEosStreamProcessor(this.parallelConsumerOptions);
	}


	private ParallelConsumerOptions<K, V> parallelConsumerOptions(Consumer<K, V> consumer,
																  Producer<K, V> producer) {
		final ParallelConsumerCallback<K, V> callback = parallelConsumerContext.parallelConsumerCallback();
		if (callback instanceof PollAndProduceMany<K,V> ||
			callback instanceof PollAndProduce<K,V>) {
			return parallelConsumerContext.getParallelConsumerOptions(consumer, producer);
		} else {
			return parallelConsumerContext.getParallelConsumerOptions(consumer);
		}
	}

	@Override
	public void start() {
		subscribe();

		final ParallelConsumerCallback<K, V> callback0 = parallelConsumerContext.parallelConsumerCallback();

		if (callback0 instanceof ResultConsumerCallback) {
			if (callback0 instanceof PollAndProduceManyResult<K, V>) {
				final PollAndProduceManyResult<K, V> callback =
						(PollAndProduceManyResult<K, V>) callback0;

				this.parallelConsumer.pollAndProduceMany(callback::accept, callback::resultConsumer);
			}
			else if (callback0 instanceof PollAndProduce<K, V>) {
				final PollAndProduceResult<K, V> callback =
						(PollAndProduceResult<K, V>) callback0;

				this.parallelConsumer.pollAndProduce(callback::accept, callback::resultConsumer);
			}
			else {
				throw new UnsupportedOperationException();
			}
		} else {
			if (callback0 instanceof PollAndProduceMany<K, V>) {
				final PollAndProduceMany<K, V> callback =
						(PollAndProduceMany<K, V>) callback0;

				this.parallelConsumer.pollAndProduceMany(callback::accept);
			}
			else if (callback0 instanceof PollAndProduce<K, V>) {
				final PollAndProduce<K, V> callback =
						(PollAndProduce<K, V>) callback0;

				this.parallelConsumer.pollAndProduce(callback::accept);
			}
			else if (callback0 instanceof Poll<K, V>) {
				final Poll<K, V> callback = (Poll<K, V>) callback0;

				this.parallelConsumer.poll(callback::accept);
			}
			else {
				throw new UnsupportedOperationException();
			}
		}
		this.running = true;
	}

	@Override
	public void stop() {
		final ParallelConsumerCallback<K, V> callback =
				this.parallelConsumerContext.parallelConsumerCallback();
		final DrainingMode drainingMode = callback.drainingMode();
		final Duration duration = callback.drainTimeOut();

		this.parallelConsumer.close(duration, drainingMode);
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	private void subscribe() {
		final ParallelConsumerCallback<K, V> callback = this.parallelConsumerContext.parallelConsumerCallback();

		final List<String> topics = callback.getTopics();
		final ConsumerRebalanceListener rebalanceListener = callback.getRebalanceListener();

		if (topics != null && !topics.isEmpty()) {
			subscribe(topics, rebalanceListener);
		}
		else {
			subscribe(callback.getSubscribeTopicsPattern(), rebalanceListener);
		}
	}

	private void subscribe(Collection<String> topics, ConsumerRebalanceListener listenerCallback){
		if (listenerCallback == null) {
			this.parallelConsumer.subscribe(topics);
		}
		else {
			this.parallelConsumer.subscribe(topics, listenerCallback);
		}
	}

	private void subscribe(Pattern pattern, ConsumerRebalanceListener listenerCallback) {
		if (listenerCallback == null) {
			this.parallelConsumer.subscribe(pattern);
		}
		else {
			this.parallelConsumer.subscribe(pattern, listenerCallback);
		}
	}

}
