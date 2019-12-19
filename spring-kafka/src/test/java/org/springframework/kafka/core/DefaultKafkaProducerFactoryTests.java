/*
 * Copyright 2018-2019 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author Gary Russell
 * @since 1.3.5
 *
 */
public class DefaultKafkaProducerFactoryTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testProducerClosedAfterBadTransition() throws Exception {
		final Producer producer = mock(Producer.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createTransactionalProducer(String txIdPrefix) {
				producer.initTransactions();
				BlockingQueue<Producer> cache = getCache(txIdPrefix);
				Producer cached = cache.poll();
				return cached == null ? new CloseSafeProducer(producer, cache) : cached;
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
		transactionTemplate.execute(s -> {
			kafkaTemplate.send("foo", "bar");
			return null;
		});
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache).hasSize(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue).hasSize(1);
		try {
			transactionTemplate.execute(s -> {
				return null;
			});
		}
		catch (CannotCreateTransactionException e) {
			assertThat(e.getCause().getMessage()).contains("Invalid transition");
		}
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

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testResetSingle() {
		final Producer producer = mock(Producer.class);
		ProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createKafkaProducer() {
				return producer;
			}

		};
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNotNull();
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(0);
		pf.reset();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		verify(producer).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testResetTx() throws Exception {
		final Producer producer = mock(Producer.class);
		ApplicationContext ctx = mock(ApplicationContext.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			@Override
			protected Producer createTransactionalProducer(String txIdPrefix) {
				producer.initTransactions();
				BlockingQueue<Producer> cache = getCache(txIdPrefix);
				Producer cached = cache.poll();
				return cached == null ? new CloseSafeProducer(producer, cache) : cached;
			}

		};
		pf.setApplicationContext(ctx);
		pf.setTransactionIdPrefix("foo");
		Producer aProducer = pf.createProducer();
		assertThat(aProducer).isNotNull();
		aProducer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "producer")).isNull();
		Map<?, ?> cache = KafkaTestUtils.getPropertyValue(pf, "cache", Map.class);
		assertThat(cache.size()).isEqualTo(1);
		Queue queue = (Queue) cache.get("foo");
		assertThat(queue.size()).isEqualTo(1);
		pf.onApplicationEvent(new ContextStoppedEvent(ctx));
		assertThat(queue.size()).isEqualTo(0);
		verify(producer).close(any(Duration.class));
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testThreadLocal() {
		final Producer producer = mock(Producer.class);
		DefaultKafkaProducerFactory pf = new DefaultKafkaProducerFactory(new HashMap<>()) {

			boolean created;

			@Override
			protected Producer createKafkaProducer() {
				assertThat(this.created).isFalse();
				this.created = true;
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
		assertThat(KafkaTestUtils.getPropertyValue(pf, "threadBoundProducers", ThreadLocal.class).get()).isNotNull();
		pf.closeThreadBoundProducer();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "threadBoundProducers", ThreadLocal.class).get()).isNull();
		verify(producer).close(any(Duration.class));
	}

}
