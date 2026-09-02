/*
 * Copyright 2026-present the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for {@link ContainerProperties#setAwaitAsyncResultsOnStop(boolean)}: in-flight
 * async listener results are awaited within the shutdown timeout and cancelled after it.
 *
 * @author Nikita Kibitkin
 *
 * @since 4.0.8
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { AwaitAsyncResultsOnStopTests.FUTURE_TOPIC, AwaitAsyncResultsOnStopTests.FUTURE_CANCEL_TOPIC,
		AwaitAsyncResultsOnStopTests.MONO_TOPIC, AwaitAsyncResultsOnStopTests.MONO_CANCEL_TOPIC,
		AwaitAsyncResultsOnStopTests.CONCURRENT_TOPIC, AwaitAsyncResultsOnStopTests.DISABLED_TOPIC,
		AwaitAsyncResultsOnStopTests.BLOCKING_TOPIC }, partitions = 2)
public class AwaitAsyncResultsOnStopTests {

	static final String FUTURE_TOPIC = "aaros.future";

	static final String FUTURE_CANCEL_TOPIC = "aaros.future.cancel";

	static final String MONO_TOPIC = "aaros.mono";

	static final String MONO_CANCEL_TOPIC = "aaros.mono.cancel";

	static final String CONCURRENT_TOPIC = "aaros.concurrent";

	static final String DISABLED_TOPIC = "aaros.disabled";

	static final String BLOCKING_TOPIC = "aaros.blocking";

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private Listener listener;

	@Autowired
	private Config config;

	@Autowired
	private EmbeddedKafkaBroker broker;

	@Test
	void futureCompletedWithinShutdownTimeoutIsAcknowledged() throws Exception {
		assertCompletedWithinTimeout("future", FUTURE_TOPIC, this.listener.future);
	}

	@Test
	void monoCompletedWithinShutdownTimeoutIsAcknowledged() throws Exception {
		assertCompletedWithinTimeout("mono", MONO_TOPIC, this.listener.mono);
	}

	@Test
	void futureNotCompletedWithinShutdownTimeoutIsCancelledAndRedelivered() throws Exception {
		assertCancelledAndRedelivered("futureCancel", FUTURE_CANCEL_TOPIC, this.listener.futureCancel);
	}

	@Test
	void monoNotCompletedWithinShutdownTimeoutIsCancelledAndRedelivered() throws Exception {
		assertCancelledAndRedelivered("monoCancel", MONO_CANCEL_TOPIC, this.listener.monoCancel);
	}

	@Test
	void eachChildContainerAwaitsItsOwnResults() throws Exception {
		AsyncWork work = this.listener.concurrent;
		MessageListenerContainer container = this.registry.getListenerContainer("concurrent");
		ContainerTestUtils.waitForAssignment(container, 2);
		this.template.send(CONCURRENT_TOPIC, 0, null, "p0").get(10, TimeUnit.SECONDS);
		this.template.send(CONCURRENT_TOPIC, 1, null, "p1").get(10, TimeUnit.SECONDS);
		await().until(() -> work.started.get() == 2);
		CountDownLatch stopped = new CountDownLatch(1);
		AtomicInteger doneAtStop = new AtomicInteger(-1);
		container.stop(() -> {
			doneAtStop.set(work.done.get());
			stopped.countDown();
		});
		assertThat(stopped.await(1, TimeUnit.SECONDS)).isFalse();
		work.proceed.countDown();
		assertThat(stopped.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(doneAtStop).hasValue(2);
		assertThat(committedOffset("concurrent", CONCURRENT_TOPIC, 0)).isEqualTo(1L);
		assertThat(committedOffset("concurrent", CONCURRENT_TOPIC, 1)).isEqualTo(1L);
	}

	@Test
	void blockingStopCommitsCompletedResultsBeforeReturning() throws Exception {
		AsyncWork p0 = this.listener.blockingP0;
		AsyncWork p1 = this.listener.blockingP1;
		// both records must arrive in the same poll: with out of order acks the consumer is
		// paused after the first unacked record, so send before starting the container
		this.template.send(BLOCKING_TOPIC, 0, null, "p0").get(10, TimeUnit.SECONDS);
		this.template.send(BLOCKING_TOPIC, 1, null, "p1").get(10, TimeUnit.SECONDS);
		MessageListenerContainer container = this.registry.getListenerContainer("blocking");
		container.start();
		ContainerTestUtils.waitForAssignment(container, 2);
		await().untilAsserted(() -> {
			assertThat(p0.started).hasValue(1);
			assertThat(p1.started).hasValue(1);
		});
		// p0 completes 500ms into the stop, p1 never completes and is cancelled at the timeout
		Thread releaser = new Thread(() -> {
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			p0.proceed.countDown();
		});
		releaser.start();
		long stopRequested = System.currentTimeMillis();
		container.stop();
		long stopReturned = System.currentTimeMillis();
		releaser.join();
		assertThat(stopReturned - stopRequested).isGreaterThanOrEqualTo(1900);
		Long p0Committed = CommitTimes.COMMITS.get(new TopicPartition(BLOCKING_TOPIC, 0));
		assertThat(p0Committed).isNotNull();
		assertThat(p0Committed - stopRequested).isLessThan(1500);
		assertThat(p0Committed).isLessThanOrEqualTo(stopReturned);
		assertThat(CommitTimes.COMMITS).doesNotContainKey(new TopicPartition(BLOCKING_TOPIC, 1));
		assertThat(p1.cancelled.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(committedOffset("blocking", BLOCKING_TOPIC, 0)).isEqualTo(1L);
		assertThat(committedOffset("blocking", BLOCKING_TOPIC, 1)).isNull();
	}

	@Test
	void stopDoesNotWaitWhenDisabled() throws Exception {
		AsyncWork work = this.listener.disabled;
		this.template.send(DISABLED_TOPIC, 0, null, "foo").get(10, TimeUnit.SECONDS);
		await().until(() -> work.started.get() == 1);
		CountDownLatch stopped = new CountDownLatch(1);
		AtomicInteger doneAtStop = new AtomicInteger(-1);
		try {
			this.registry.getListenerContainer("disabled").stop(() -> {
				doneAtStop.set(work.done.get());
				stopped.countDown();
			});
			assertThat(stopped.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(doneAtStop).hasValue(0);
		}
		finally {
			work.proceed.countDown();
		}
	}

	private void assertCompletedWithinTimeout(String id, String topic, AsyncWork work) throws Exception {
		this.template.send(topic, 0, null, "foo").get(10, TimeUnit.SECONDS);
		await().until(() -> work.started.get() == 1);
		CountDownLatch stopped = new CountDownLatch(1);
		AtomicInteger doneAtStop = new AtomicInteger(-1);
		this.registry.getListenerContainer(id).stop(() -> {
			doneAtStop.set(work.done.get());
			stopped.countDown();
		});
		// the container waits for the in-flight result
		assertThat(stopped.await(1, TimeUnit.SECONDS)).isFalse();
		work.proceed.countDown();
		assertThat(stopped.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(doneAtStop).hasValue(1);
		assertThat(committedOffset(id, topic, 0)).isEqualTo(1L);
	}

	private void assertCancelledAndRedelivered(String id, String topic, AsyncWork work) throws Exception {
		this.template.send(topic, 0, null, "foo").get(10, TimeUnit.SECONDS);
		await().until(() -> work.started.get() == 1);
		MessageListenerContainer container = this.registry.getListenerContainer(id);
		CountDownLatch stopped = new CountDownLatch(1);
		container.stop(stopped::countDown);
		assertThat(stopped.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(work.cancelled.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(work.done).hasValue(0);
		assertThat(this.config.errorHandlerCalls).hasValue(0);
		assertThat(committedOffset(id, topic, 0)).isNull();
		// the cancelled record is redelivered on restart
		container.start();
		await().until(() -> work.started.get() == 2);
		work.proceed.countDown();
		await().untilAsserted(() -> assertThat(committedOffset(id, topic, 0)).isEqualTo(1L));
	}

	private @Nullable Long committedOffset(String group, String topic, int partition) throws Exception {
		OffsetAndMetadata committed = KafkaTestUtils.getCurrentOffset(this.broker.getBrokersAsString(), group,
				topic, partition);
		return committed == null ? null : committed.offset();
	}

	/**
	 * Async work that parks until released; {@code started} and {@code done} count the
	 * deliveries that started and completed. An interrupt while parked (Reactor
	 * interrupts the worker when the {@code Mono} is cancelled) ends the work without
	 * completing it.
	 */
	static class AsyncWork {

		final AtomicInteger started = new AtomicInteger();

		final CountDownLatch proceed = new CountDownLatch(1);

		final AtomicInteger done = new AtomicInteger();

		final CountDownLatch cancelled = new CountDownLatch(1);

		CompletableFuture<Void> future() {
			CompletableFuture<Void> future = CompletableFuture.runAsync(this::run);
			future.whenComplete((r, t) -> {
				if (t instanceof CancellationException) {
					this.cancelled.countDown();
				}
			});
			return future;
		}

		Mono<Void> mono() {
			return Mono.<Void>fromRunnable(this::run)
					.subscribeOn(Schedulers.boundedElastic())
					.doOnCancel(this.cancelled::countDown);
		}

		private void run() {
			this.started.incrementAndGet();
			try {
				this.proceed.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			this.done.incrementAndGet();
		}

	}

	public static class Listener {

		final AsyncWork future = new AsyncWork();

		final AsyncWork futureCancel = new AsyncWork();

		final AsyncWork mono = new AsyncWork();

		final AsyncWork monoCancel = new AsyncWork();

		final AsyncWork concurrent = new AsyncWork();

		final AsyncWork disabled = new AsyncWork();

		final AsyncWork blockingP0 = new AsyncWork();

		final AsyncWork blockingP1 = new AsyncWork();

		@KafkaListener(id = "future", topics = FUTURE_TOPIC, containerFactory = "awaitOnStopFactory",
				errorHandler = "errorHandler")
		public CompletableFuture<Void> future(String in) {
			return this.future.future();
		}

		@KafkaListener(id = "futureCancel", topics = FUTURE_CANCEL_TOPIC, containerFactory = "awaitOnStopFactory",
				errorHandler = "errorHandler")
		public CompletableFuture<Void> futureCancel(String in) {
			return this.futureCancel.future();
		}

		@KafkaListener(id = "mono", topics = MONO_TOPIC, containerFactory = "awaitOnStopFactory",
				errorHandler = "errorHandler")
		public Mono<Void> mono(String in) {
			return this.mono.mono();
		}

		@KafkaListener(id = "monoCancel", topics = MONO_CANCEL_TOPIC, containerFactory = "awaitOnStopFactory",
				errorHandler = "errorHandler")
		public Mono<Void> monoCancel(String in) {
			return this.monoCancel.mono();
		}

		@KafkaListener(id = "concurrent", topics = CONCURRENT_TOPIC, containerFactory = "awaitOnStopFactory",
				concurrency = "2")
		public CompletableFuture<Void> concurrent(String in) {
			return this.concurrent.future();
		}

		@KafkaListener(id = "disabled", topics = DISABLED_TOPIC, containerFactory = "kafkaListenerContainerFactory")
		public CompletableFuture<Void> disabled(String in) {
			return this.disabled.future();
		}

		@KafkaListener(id = "blocking", topics = BLOCKING_TOPIC, containerFactory = "awaitOnStopFactory",
				autoStartup = "false",
				properties = "interceptor.classes=org.springframework.kafka.listener.AwaitAsyncResultsOnStopTests$CommitTimes")
		public CompletableFuture<Void> blocking(String in) {
			return "p0".equals(in) ? this.blockingP0.future() : this.blockingP1.future();
		}

	}

	/**
	 * Records when each partition's offset was last committed.
	 */
	public static class CommitTimes implements ConsumerInterceptor<Integer, String> {

		static final Map<TopicPartition, Long> COMMITS = new ConcurrentHashMap<>();

		@Override
		public ConsumerRecords<Integer, String> onConsume(ConsumerRecords<Integer, String> records) {
			return records;
		}

		@Override
		public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
			offsets.keySet().forEach(tp -> COMMITS.put(tp, System.currentTimeMillis()));
		}

		@Override
		public void close() {
		}

		@Override
		public void configure(Map<String, ?> configs) {
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		final AtomicInteger errorHandlerCalls = new AtomicInteger();

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean
		public KafkaListenerErrorHandler errorHandler() {
			return (message, exception) -> {
				this.errorHandlerCalls.incrementAndGet();
				return null;
			};
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> awaitOnStopFactory(
				ConsumerFactory<Integer, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.getContainerProperties().setAwaitAsyncResultsOnStop(true);
			factory.getContainerProperties().setShutdownTimeout(2000);
			return factory;
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.getContainerProperties().setShutdownTimeout(2000);
			return factory;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "aaros", false);
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker));
		}

		@Bean
		public KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}

	}

}
