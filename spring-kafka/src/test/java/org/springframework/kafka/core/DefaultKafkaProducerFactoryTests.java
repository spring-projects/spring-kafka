/*
 * Copyright 2018-present the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.ProducerFactory.Listener;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Adrian Gygax
 *
 * @since 1.3.5
 */
public class DefaultKafkaProducerFactoryTests {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	void testProducerClosedAfterBadTransition() {
		final Producer producer = mock(Producer.class);
		given(producer.send(any(), any())).willReturn(new CompletableFuture());
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		pf.setTransactionIdPrefix("foo");

		final AtomicInteger flag = new AtomicInteger();
		willAnswer(i -> {
			if (flag.incrementAndGet() == 2) {
				throw new KafkaException("Invalid transition ...");
			}
			return null;
		}).given(producer).beginTransaction();

		final KafkaTemplate kafkaTemplate = new KafkaTemplate(pf);
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		TransactionTemplate transactionTemplate = new TransactionTemplate(tm);
		transactionTemplate.executeWithoutResult(s -> kafkaTemplate.send("foo", "bar"));
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache).hasSize(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue).hasSize(1);
		assertThatExceptionOfType(CannotCreateTransactionException.class)
				.isThrownBy(() -> transactionTemplate.executeWithoutResult(s -> {
				}))
				.withStackTraceContaining("Invalid transition");

		assertThat(queue).hasSize(0);

		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).initTransactions();
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).send(any(), any());
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		inOrder.verifyNoMoreInteractions();
		pf.destroy();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	void testMaxCacheProducerClosedAfterBadTransition() {
		final Producer producer = mock(Producer.class);
		given(producer.send(any(), any())).willReturn(new CompletableFuture());
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		pf.setTransactionIdPrefix("foo");
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(2);
		pf.setTransactionIdSuffixStrategy(suffixStrategy);

		final AtomicInteger flag = new AtomicInteger();
		willAnswer(i -> {
			if (flag.incrementAndGet() == 2) {
				throw new KafkaException("Invalid begin transition ...");
			}
			return null;
		}).given(producer).beginTransaction();

		final AtomicInteger commitFlag = new AtomicInteger();
		willAnswer(i -> {
			if (commitFlag.incrementAndGet() == 2) {
				throw new KafkaException("Invalid commit transition ...");
			}
			return null;
		}).given(producer).commitTransaction();

		final AtomicInteger abortFlag = new AtomicInteger();
		willAnswer(i -> {
			if (abortFlag.incrementAndGet() == 1) {
				throw new KafkaException("Invalid abort transition ...");
			}
			return null;
		}).given(producer).abortTransaction();

		final AtomicInteger initFlag = new AtomicInteger();
		willAnswer(i -> {
			if (initFlag.incrementAndGet() == 4) {
				throw new KafkaException("Invalid init transition ...");
			}
			return null;
		}).given(producer).initTransactions();

		final KafkaTemplate kafkaTemplate = new KafkaTemplate(pf);
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		TransactionTemplate transactionTemplate = new TransactionTemplate(tm);
		transactionTemplate.executeWithoutResult(s -> kafkaTemplate.send("foo", "bar"));
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache).hasSize(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue).hasSize(1);
		Map<?, ?> suffixCache = KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class);
		assertThat(suffixCache).hasSize(1);
		Queue suffixQueue = (Queue) suffixCache.get("foo");
		assertThat(suffixQueue).hasSize(1);

		assertThatExceptionOfType(CannotCreateTransactionException.class)
				.isThrownBy(() -> transactionTemplate.executeWithoutResult(s -> {
				}));
		assertThat(queue).hasSize(0);
		assertThat(suffixQueue).hasSize(2);

		assertThatExceptionOfType(KafkaException.class)
				.isThrownBy(() -> transactionTemplate.executeWithoutResult(s -> {
				}))
				.withStackTraceContaining("Invalid commit transition");
		assertThat(queue).hasSize(0);
		assertThat(suffixQueue).hasSize(2);

		assertThatExceptionOfType(KafkaException.class)
				.isThrownBy(() -> {
					transactionTemplate.execute(s -> {
						throw new RuntimeException("rollback exception");
					});
				})
				.withStackTraceContaining("Invalid abort transition");
		assertThat(queue).hasSize(0);
		assertThat(suffixQueue).hasSize(2);

		assertThatExceptionOfType(CannotCreateTransactionException.class)
				.isThrownBy(() -> transactionTemplate.executeWithoutResult(s -> {

				}))
				.withStackTraceContaining("Could not create Kafka transaction");
		assertThat(queue).hasSize(0);
		assertThat(suffixQueue).hasSize(2);

		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).initTransactions();
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).send(any(), any());
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		inOrder.verify(producer).initTransactions();
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		inOrder.verify(producer).initTransactions();
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).abortTransaction();
		inOrder.verify(producer).close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		inOrder.verify(producer).initTransactions();
		inOrder.verify(producer).close(DefaultKafkaProducerFactory.DEFAULT_PHYSICAL_CLOSE_TIMEOUT);
		inOrder.verifyNoMoreInteractions();
		pf.destroy();
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testResetSingle() throws InterruptedException {
		final Producer producer = mock(Producer.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		aProducer.close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNotNull();
		pf.setMaxAge(Duration.ofMillis(10));
		Thread.sleep(50);
		aProducer = pf.createProducer();
		assertThat(aProducer).isNotSameAs(bProducer);
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(0);
		pf.reset();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		verify(producer, times(2)).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void singleLifecycle() throws InterruptedException {
		final Producer producer = mock(Producer.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		aProducer.close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNotNull();
		pf.setMaxAge(Duration.ofMillis(10));
		Thread.sleep(50);
		aProducer = pf.createProducer();
		assertThat(aProducer).isNotSameAs(bProducer);
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(0);
		pf.stop();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		verify(producer, times(2)).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testResetTx() throws Exception {
		final Producer producer = mock(Producer.class);
		ApplicationContext ctx = mock(ApplicationContext.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		pf.setApplicationContext(ctx);
		pf.setTransactionIdPrefix("foo");
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(3);
		pf.setTransactionIdSuffixStrategy(suffixStrategy);
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		bProducer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue.size()).isEqualTo(1);
		Map<?, ?> suffixCache = KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class);
		assertThat(suffixCache.size()).isEqualTo(1);
		Queue suffixQueue = (Queue) suffixCache.get("foo");
		assertThat(suffixQueue.size()).isEqualTo(2);
		pf.setMaxAge(Duration.ofMillis(10));
		Thread.sleep(50);
		aProducer = pf.createProducer();
		assertThat(aProducer).isNotSameAs(bProducer);
		pf.onApplicationEvent(new ContextStoppedEvent(ctx));
		assertThat(queue.size()).isEqualTo(0);
		assertThat(suffixCache.size()).isEqualTo(1);
		Queue suffixQueue2 = (Queue) suffixCache.get("foo");
		assertThat(suffixQueue2.size()).isEqualTo(2);
		verify(producer).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void txLifecycle() throws Exception {
		final Producer producer = mock(Producer.class);
		ApplicationContext ctx = mock(ApplicationContext.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		pf.setApplicationContext(ctx);
		pf.setTransactionIdPrefix("foo");
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		bProducer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue.size()).isEqualTo(1);
		pf.setMaxAge(Duration.ofMillis(10));
		Thread.sleep(50);
		aProducer = pf.createProducer();
		assertThat(aProducer).isNotSameAs(bProducer);
		pf.stop();
		assertThat(queue.size()).isEqualTo(0);
		assertThat(cache.size()).isEqualTo(0);
		verify(producer).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void dontReturnToCacheAfterReset() {
		final Producer producer = mock(Producer.class);
		ApplicationContext ctx = mock(ApplicationContext.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		pf.setApplicationContext(ctx);
		pf.setTransactionIdPrefix("foo");
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(3);
		pf.setTransactionIdSuffixStrategy(suffixStrategy);
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		bProducer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue.size()).isEqualTo(1);
		Map<?, ?> suffixCache = KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class);
		assertThat(suffixCache.size()).isEqualTo(1);
		Queue suffixQueue = (Queue) suffixCache.get("foo");
		assertThat(suffixQueue.size()).isEqualTo(2);
		bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		assertThat(queue.size()).isEqualTo(0);
		pf.reset();
		bProducer.close();
		assertThat(queue.size()).isEqualTo(0);
		assertThat(suffixCache.size()).isEqualTo(1);
		Queue suffixQueue2 = (Queue) suffixCache.get("foo");
		assertThat(suffixQueue2.size()).isEqualTo(3);
		pf.destroy();
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testThreadLocal() throws InterruptedException {
		final Producer producer = mock(Producer.class);
		AtomicBoolean created = new AtomicBoolean();
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createKafkaProducer() {
				assertThat(created.get()).isFalse();
				created.set(true);
				return producer;
			}

		};
		pf.setProducerPerThread(true);
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		bProducer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "threadBoundProducers", Map.class).get(Thread.currentThread()))
				.isNotNull();
		pf.setMaxAge(Duration.ofMillis(10));
		Thread.sleep(50);
		created.set(false);
		aProducer = pf.createProducer();
		assertThat(aProducer).isNotSameAs(bProducer);
		pf.closeThreadBoundProducer();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "threadBoundProducers", Map.class).get(Thread.currentThread()))
				.isNull();
		verify(producer, times(2)).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void threadLocalLifecycle() {
		final Producer producer = mock(Producer.class);
		AtomicBoolean created = new AtomicBoolean();
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createKafkaProducer() {
				assertThat(created.get()).isFalse();
				created.set(true);
				return producer;
			}

		};
		pf.setProducerPerThread(true);
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "threadBoundProducers", Map.class).get(Thread.currentThread()))
				.isNotNull();
		pf.stop();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "threadBoundProducers", Map.class)).hasSize(0);
		verify(producer).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testThreadLocalReset() {
		Producer producer1 = mock(Producer.class);
		Producer producer2 = mock(Producer.class);
		ProducerFactory mockPf = mock(ProducerFactory.class);
		given(mockPf.createProducer()).willReturn(producer1, producer2);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createKafkaProducer() {
				return mockPf.createProducer();
			}

		};
		pf.setProducerPerThread(true);
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		bProducer.close();
		pf.reset();
		bProducer = pf.createProducer();
		assertThat(bProducer).isNotSameAs(aProducer);
		bProducer.close();
		verify(producer1).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testUnknownProducerIdException() {
		final Producer producer1 = mock(Producer.class);
		willAnswer(inv -> {
			((Callback) inv.getArgument(1)).onCompletion(null, new UnknownProducerIdException("test"));
			return null;
		}).given(producer1).send(isNull(), any());
		final Producer producer2 = mock(Producer.class);
		ProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			private final AtomicBoolean first = new AtomicBoolean(true);

			@Override
			protected Producer createKafkaProducer() {
				return this.first.getAndSet(false) ? producer1 : producer2;
			}

		};
		final Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		Producer bProducer = pf.createProducer();
		assertThat(bProducer).isSameAs(aProducer);
		aProducer.send(null, (meta, ex) -> {
		});
		aProducer.close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		bProducer = pf.createProducer();
		verify(producer1).close(any(Duration.class));
		assertThat(bProducer).isNotSameAs(aProducer);
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testNoProducerException() {
		final Producer producer = mock(Producer.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		pf.setTransactionIdPrefix("foo");
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(2);
		pf.setTransactionIdSuffixStrategy(suffixStrategy);
		Map<?, ?> suffixCache = KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class);
		pf.createProducer();
		Queue queue = (Queue) suffixCache.get("foo");
		assertThat(queue).hasSize(1);
		pf.createProducer();
		assertThat(queue).hasSize(0);
		assertThatExceptionOfType(NoProducerAvailableException.class).isThrownBy(() -> pf.createProducer());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	void listener() {
		Producer producer = mock(Producer.class);
		Map<MetricName, ? extends Metric> metrics = new HashMap<>();
		metrics.put(new MetricName("test", "group", "desc", Collections.singletonMap("client-id", "foo-0")), null);
		given(producer.metrics()).willReturn(metrics);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(Collections.EMPTY_MAP) {

			@Override
			protected Producer createRawProducer(Map configs) {
				return producer;
			}

		};
		List<String> adds = new ArrayList<>();
		List<String> removals = new ArrayList<>();
		pf.addListener(new Listener() {

			@Override
			public void producerAdded(String id, Producer producer) {
				adds.add(id);
			}

			@Override
			public void producerRemoved(String id, Producer producer) {
				removals.add(id);
			}

		});
		pf.setBeanName("pf");

		pf.createProducer().close();
		assertThat(adds).hasSize(1);
		assertThat(adds.get(0)).isEqualTo("pf.foo-0");
		assertThat(removals).hasSize(0);
		pf.createProducer().close();
		assertThat(adds).hasSize(1);
		assertThat(removals).hasSize(0);
		pf.reset();
		assertThat(adds).hasSize(1);
		assertThat(removals).hasSize(1);

		pf.createProducer("tx").close();
		assertThat(adds).hasSize(2);
		assertThat(removals).hasSize(1);
		pf.createProducer("tx").close();
		assertThat(adds).hasSize(2);
		assertThat(removals).hasSize(1);
		pf.reset();
		assertThat(adds).hasSize(2);
		assertThat(removals).hasSize(2);

		pf.createProducer("tx").close();
		assertThat(adds).hasSize(3);
		assertThat(removals).hasSize(2);
		pf.createProducer("tx").close();
		assertThat(adds).hasSize(3);
		assertThat(removals).hasSize(2);
		pf.reset();
		assertThat(adds).hasSize(3);
		assertThat(removals).hasSize(3);

		pf.createProducer("tx").close();
		assertThat(adds).hasSize(4);
		assertThat(removals).hasSize(3);
		pf.createProducer("tx").close();
		assertThat(adds).hasSize(4);
		assertThat(removals).hasSize(3);
		pf.reset();
		assertThat(adds).hasSize(4);
		assertThat(removals).hasSize(4);

		pf.setProducerPerThread(true);
		pf.createProducer().close();
		assertThat(adds).hasSize(5);
		assertThat(removals).hasSize(4);
		pf.createProducer().close();
		assertThat(adds).hasSize(5);
		assertThat(removals).hasSize(4);
		pf.closeThreadBoundProducer();
		assertThat(adds).hasSize(5);
		assertThat(removals).hasSize(5);
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testBootstrapSupplier() {
		final Producer producer = mock(Producer.class);
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				configPassedToKafkaConsumer.clear();
				configPassedToKafkaConsumer.putAll(configs);
				return producer;
			}

		};
		pf.setBootstrapServersSupplier(() -> "foo");
		Producer aProducer = pf.createProducer();
		assertThat(configPassedToKafkaConsumer.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("foo");
		pf.setBootstrapServersSupplier(() -> "bar");
		pf.setTransactionIdPrefix("tx.");
		aProducer = pf.createProducer();
		assertThat(configPassedToKafkaConsumer.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("bar");
		pf.destroy();
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void testTransactionId() throws InterruptedException {
		final Producer producer = mock(Producer.class);
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createRawProducer(Map configs) {
				configPassedToKafkaConsumer.clear();
				configPassedToKafkaConsumer.putAll(configs);
				return producer;
			}

		};
		pf.setBootstrapServersSupplier(() -> "foo");
		pf.setTransactionIdPrefix("tx.");
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(2);
		pf.setTransactionIdSuffixStrategy(suffixStrategy);
		Producer aProducer = pf.createProducer();
		assertThat(configPassedToKafkaConsumer.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo("tx.0");
		Producer bProducer = pf.createProducer();
		assertThat(configPassedToKafkaConsumer.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo("tx.1");
		bProducer.close(Duration.ofSeconds(20));
		pf.setMaxAge(Duration.ofMillis(10));
		Thread.sleep(50);
		Producer cProducer = pf.createProducer();
		assertThat(configPassedToKafkaConsumer.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo("tx.1");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	void configUpdates() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory<>(configs);
		assertThat(pf.getConfigurationProperties()).hasSize(1);
		configs.remove(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
		configs.put(ProducerConfig.ACKS_CONFIG, "all");
		pf.updateConfigs(configs);
		assertThat(pf.getConfigurationProperties()).hasSize(2);
		pf.removeConfig(ProducerConfig.ACKS_CONFIG);
		assertThat(pf.getConfigurationProperties()).hasSize(1);
		configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-");
		assertThatIllegalArgumentException().isThrownBy(() -> pf.updateConfigs(configs));
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configs.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
		DefaultKafkaProducerFactory pf1 = new DefaultKafkaProducerFactory<>(configs);
		assertThat(pf1.getConfigurationProperties()).hasSize(4);
		configs.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId2");
		configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx2-");
		pf1.updateConfigs(configs);
		assertThat(pf1.getConfigurationProperties()).hasSize(4);
		assertThat(KafkaTestUtils.getPropertyValue(pf1, "clientIdPrefix")).isEqualTo("clientId2");
		assertThat(KafkaTestUtils.getPropertyValue(pf1, "transactionIdPrefix")).isEqualTo("tx2-");
		configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, null);
		assertThatIllegalArgumentException().isThrownBy(() -> pf1.updateConfigs(configs));
		assertThatCode(() -> pf1.updateConfigs(Collections.singletonMap(null, null))).doesNotThrowAnyException();
	}

	@SuppressWarnings("unchecked")
	@Test
	void configSerializer() {
		Serializer<String> key = mock(Serializer.class);
		Serializer<String> value = mock(Serializer.class);
		Map<String, Object> config = new HashMap<>();
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(config, key, value);
		Serializer<String> keySerializer = pf.getKeySerializer();
		assertThat(keySerializer).isSameAs(key);
		keySerializer = pf.getKeySerializerSupplier().get();
		assertThat(keySerializer).isSameAs(key);
		verify(key).configure(any(), eq(true));
		Serializer<String> valueSerializer = pf.getValueSerializer();
		assertThat(valueSerializer).isSameAs(value);
		value = pf.getValueSerializerSupplier().get();
		assertThat(valueSerializer).isSameAs(value);
		verify(value).configure(any(), eq(false));
	}

	@Test
	void testConfigOverridesOfTransactionalProducer() {
		final Map<String, Object> producerFactoryConfigs = Map.of("linger.ms", 100);
		final Map<String, Object> producerConfigs = new HashMap<>();
		final DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerFactoryConfigs) {

			@Override
			protected Map<String, Object> getTxProducerConfigs(String transactionId) {
				final Map<String, Object> newProducerConfigs = super.getTxProducerConfigs(transactionId);
				newProducerConfigs.put("linger.ms", 200);
				return newProducerConfigs;
			}

			@Override
			protected Producer<String, String> createRawProducer(Map<String, Object> rawConfigs) {
				producerConfigs.putAll(rawConfigs);
				return new MockProducer<>();
			}
		};
		pf.setTransactionIdPrefix("tx-prefix");
		pf.createProducer();
		assertThat(producerFactoryConfigs).containsEntry("linger.ms", 100);
		assertThat(producerConfigs).containsEntry("linger.ms", 200);
	}

	@Test
	void testDefaultClientIdPrefixIsSpringBootApplicationName() {
		final DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(Map.of());
		final Environment environment = mock(Environment.class);
		given(environment.getProperty("spring.application.name")).willReturn("appname");
		final ApplicationContext applicationContext = mock(ApplicationContext.class);
		given(applicationContext.getEnvironment()).willReturn(environment);
		pf.setApplicationContext(applicationContext);
		assertThat(pf.getProducerConfigs()).containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "appname-producer-1");
	}

	@Test
	void testExplicitClientIdPrefixOverridesDefault() {
		final DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(Map.of(ProducerConfig.CLIENT_ID_CONFIG, "clientId"));
		final Environment environment = mock(Environment.class);
		given(environment.getProperty("spring.application.name")).willReturn("appname");
		final ApplicationContext applicationContext = mock(ApplicationContext.class);
		given(applicationContext.getEnvironment()).willReturn(environment);
		pf.setApplicationContext(applicationContext);
		assertThat(pf.getProducerConfigs()).containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "clientId-1");
	}

}
