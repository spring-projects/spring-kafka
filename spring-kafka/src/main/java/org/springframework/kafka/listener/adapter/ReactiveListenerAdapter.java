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

package org.springframework.kafka.listener.adapter;

import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Reactive adapter for message listeners; the container must be configured with a manual
 * ack mode and for async acks.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public abstract class ReactiveListenerAdapter<K, V> implements AcknowledgingMessageListener<K, V>, SmartLifecycle {

	protected final LogAccessor logger = new LogAccessor(getClass()); // NOSONAR

	private final Sinks.Many<Mono<ConsumerRecord<K, V>>> sink =
			Sinks.many().unicast().onBackpressureBuffer();

	private int phase = Integer.MIN_VALUE + 100;

	private int parallel;

	private Scheduler scheduler = Schedulers.parallel();

	Disposable disposable;

	@Override
	public void onMessage(ConsumerRecord<K, V> record, @Nullable Acknowledgment ack) {
		if (this.disposable == null) {
			throw new IllegalStateException("Listener is not started");
		}
		Mono<ConsumerRecord<K, V>> mono = Mono.just(record)
				.doAfterTerminate(() -> ack.acknowledge());
		this.sink.emitNext(mono, EmitFailureHandler.FAIL_FAST);
	}

	/**
	 * Enable {@code .parallel(n)}.
	 * @param parallel the parallelism.
	 */
	public void setParallel(int parallel) {
		this.parallel = parallel;
	}

	/**
	 * When using {@link #setParallel(int)}, the scheduler to run on. Default is
	 * {@link Schedulers#parallel()}.
	 * @param scheduler the scheduler.
	 */
	public void setScheduler(Scheduler scheduler) {
		Assert.notNull(scheduler, "'scheduler' cannot be null");
		this.scheduler = scheduler;
	}


	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Set the SmartLifecyle phase - must start before the listener container.
	 * @param phase the phase.
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public void propertiesSupplier(Supplier<ContainerProperties> containerProperties) {
		ContainerProperties props = containerProperties.get();
		Assert.state(props.isAsyncAcks(), "The 'asyncAcks' property must be true to use this adapter");
		AckMode ackMode = props.getAckMode();
		Assert.state(AckMode.MANUAL.equals(ackMode) || AckMode.MANUAL_IMMEDIATE.equals(ackMode),
				"A manual AckMode is required to use this adapter");
	}

	@Override
	public void start() {
		if (this.parallel > 0) {
			this.disposable = this.sink.asFlux()
					.parallel(this.parallel)
					.runOn(this.scheduler)
					.doOnNext(mono -> {
						assemble(mono).subscribe();
					})
					.subscribe();

		}
		else {
			this.disposable = this.sink.asFlux()
					.doOnNext(mono -> {
						assemble(mono).subscribe();
					})
					.subscribe();
		}
	}

	/**
	 * Add behavior to the {@link Mono} for the {@link ConsumerRecord}. The behavior must
	 * include error handling, the record will be ackowledged unconditionally when the
	 * mono terminates.
	 * @param mono the mono.
	 * @return the assembled mono.
	 */
	public abstract Mono<ConsumerRecord<K, V>> assemble(Mono<ConsumerRecord<K, V>> mono);

	@Override
	public void stop() {
		if (this.disposable != null) {
			this.disposable.dispose();
			this.disposable = null;
		}
	}

	@Override
	public boolean isRunning() {
		return this.disposable != null;
	}

}
