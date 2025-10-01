/*
 * Copyright 2017-present the original author or authors.
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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.kafka.support.transaction.ResourcelessTransactionManager;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

/**
 * @author Gary Russell
 * @author Nakul Mishra
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
@EmbeddedKafka(topics = {KafkaTemplateTransactionTests.STRING_KEY_TOPIC,
		KafkaTemplateTransactionTests.LOCAL_TX_IN_TOPIC, KafkaTemplateTransactionTests.LOCAL_FIXED_TX_IN_TOPIC},
		brokerProperties = {"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"})
public class KafkaTemplateTransactionTests {

	public static final String STRING_KEY_TOPIC = "stringKeyTopic";

	public static final String LOCAL_TX_IN_TOPIC = "localTxInTopic";

	public static final String LOCAL_FIXED_TX_IN_TOPIC = "localFixedTxInTopic";

	private final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	@Test
	public void testLocalTransaction() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my.transaction.");
		senderProps.put(ProducerConfig.CLIENT_ID_CONFIG, "customClientId");
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "testLocalTx", false);
		consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, STRING_KEY_TOPIC, LOCAL_TX_IN_TOPIC);
		template.executeInTransaction(kt -> kt.send(LOCAL_TX_IN_TOPIC, "one"));
		ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, LOCAL_TX_IN_TOPIC);
		template.executeInTransaction(t -> {
			pf.createProducer("testCustomClientIdIsUnique").close();
			t.sendDefault("foo", "bar");
			t.sendDefault("baz", "qux");
			t.sendOffsetsToTransaction(Collections.singletonMap(
					new TopicPartition(LOCAL_TX_IN_TOPIC, singleRecord.partition()),
					new OffsetAndMetadata(singleRecord.offset() + 1L)), consumer.groupMetadata());
			assertThat(KafkaTestUtils.getPropertyValue(
					KafkaTestUtils.getPropertyValue(template, "producers", Map.class).get(Thread.currentThread()),
					"delegate.transactionManager.transactionalId")).isEqualTo("my.transaction.0");
			return null;
		});
		ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
		Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
		ConsumerRecord<String, String> record = iterator.next();
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("foo"), value("bar")));
		if (!iterator.hasNext()) {
			records = KafkaTestUtils.getRecords(consumer);
			iterator = records.iterator();
		}
		record = iterator.next();
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("baz"), value("qux")));
		// 2 log slots, 1 for the record, 1 for the commit
		assertThat(consumer.position(new TopicPartition(LOCAL_TX_IN_TOPIC, singleRecord.partition()))).isEqualTo(2L);
		consumer.close();
		assertThat(pf.getCache()).hasSize(1);
		template.setTransactionIdPrefix("tx.template.override.");
		template.executeInTransaction(t -> {
			assertThat(KafkaTestUtils.getPropertyValue(
					KafkaTestUtils.getPropertyValue(template, "producers", Map.class).get(Thread.currentThread()),
					"delegate.transactionManager.transactionalId")).isEqualTo("tx.template.override.2");
			return null;
		});
		assertThat(pf.getCache("tx.template.override.")).hasSize(1);
		pf.destroy();
		assertThat(pf.getCache()).hasSize(0);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testLocalTransactionIsFixed() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my.transaction.fixed.");
		senderProps.put(ProducerConfig.CLIENT_ID_CONFIG, "customClientIdFixed");
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(3);
		pf.setTransactionIdSuffixStrategy(suffixStrategy);
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "testLocalTxFixed", false);
		consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, STRING_KEY_TOPIC, LOCAL_FIXED_TX_IN_TOPIC);
		template.executeInTransaction(kt -> kt.send(LOCAL_FIXED_TX_IN_TOPIC, "one")); // suffix range {0-2}
		ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, LOCAL_FIXED_TX_IN_TOPIC);
		template.executeInTransaction(t -> {
			pf.createProducer("testCustomClientIdIsUniqueFixed").close(); // suffix range {3-5}
			t.sendDefault("foo", "bar");
			t.sendDefault("baz", "qux");
			t.sendOffsetsToTransaction(Collections.singletonMap(
					new TopicPartition(LOCAL_FIXED_TX_IN_TOPIC, singleRecord.partition()),
					new OffsetAndMetadata(singleRecord.offset() + 1L)), consumer.groupMetadata());
			assertThat(KafkaTestUtils.getPropertyValue(
					KafkaTestUtils.getPropertyValue(template, "producers", Map.class).get(Thread.currentThread()),
					"delegate.transactionManager.transactionalId")).isEqualTo("my.transaction.fixed.0");
			return null;
		});
		ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
		Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
		ConsumerRecord<String, String> record = iterator.next();
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("foo"), value("bar")));
		if (!iterator.hasNext()) {
			records = KafkaTestUtils.getRecords(consumer);
			iterator = records.iterator();
		}
		record = iterator.next();
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("baz"), value("qux")));
		// 2 log slots, 1 for the record, 1 for the commit
		assertThat(consumer.position(new TopicPartition(LOCAL_FIXED_TX_IN_TOPIC, singleRecord.partition()))).isEqualTo(2L);
		consumer.close();
		assertThat(pf.getCache()).hasSize(1);
		template.setTransactionIdPrefix("tx.template.override.fixed."); // suffix range {6-8}
		template.executeInTransaction(t -> {
			assertThat(KafkaTestUtils.getPropertyValue(
					KafkaTestUtils.getPropertyValue(template, "producers", Map.class).get(Thread.currentThread()),
					"delegate.transactionManager.transactionalId")).isEqualTo("tx.template.override.fixed.6");
			return null;
		});
		assertThat(pf.getCache("tx.template.override.fixed.")).hasSize(1);
		Map<?, ?> suffixCache = KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class);
		assertThat((Queue) suffixCache.get("tx.template.override.fixed.")).hasSize(2);
		assertThat(pf.getCache("testCustomClientIdIsUniqueFixed")).hasSize(1);
		assertThat((Queue) suffixCache.get("testCustomClientIdIsUniqueFixed")).hasSize(2);
		pf.destroy();
		assertThat(pf.getCache()).hasSize(0);
		assertThat(KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class)).hasSize(3);
	}

	@Test
	public void testGlobalTransaction() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		pf.setTransactionIdPrefix("my.transaction.");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "testGlobalTx", false);
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, STRING_KEY_TOPIC);
		KafkaTransactionManager<String, String> tm = new KafkaTransactionManager<>(pf);
		tm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
		new TransactionTemplate(tm)
				.executeWithoutResult(s -> {
					template.sendDefault("foo", "bar");
					template.sendDefault("baz", "qux");
				});
		ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
		Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
		ConsumerRecord<String, String> record = iterator.next();
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("foo"), value("bar")));
		if (!iterator.hasNext()) {
			records = KafkaTestUtils.getRecords(consumer);
			iterator = records.iterator();
		}
		record = iterator.next();
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("baz"), value("qux")));
		consumer.close();
		assertThat(pf.getCache()).hasSize(1);
		pf.destroy();
		assertThat(pf.getCache()).hasSize(0);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testDeclarative() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(DeclarativeConfig.class);
		Tx1 tx1 = ctx.getBean(Tx1.class);
		tx1.txMethod();
		ProducerFactory producerFactory = ctx.getBean(ProducerFactory.class);
		verify(producerFactory, times(1)).createProducer(isNull());
		verify(producerFactory, times(1)).createProducer(eq("custom.tx.prefix."));
		Producer producer1 = ctx.getBean("producer1", Producer.class);
		Producer producer2 = ctx.getBean("producer2", Producer.class);
		InOrder inOrder = inOrder(producer1, producer2);
		inOrder.verify(producer1).beginTransaction();
		inOrder.verify(producer1).send(eq(new ProducerRecord("foo", "bar")), any(Callback.class));
		inOrder.verify(producer1).send(eq(new ProducerRecord("baz", "qux")), any(Callback.class));
		inOrder.verify(producer2).beginTransaction();
		inOrder.verify(producer2).send(eq(new ProducerRecord("fiz", "buz")), any(Callback.class));
		inOrder.verify(producer2).commitTransaction();
		inOrder.verify(producer1).commitTransaction();
		ctx.close();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testDeclarativeWithMockProducer() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
				DeclarativeConfigWithMockProducer.class);
		Tx1 tx1 = ctx.getBean(Tx1.class);
		tx1.txMethod();
		MockProducer producer1 = ctx.getBean("producer1", MockProducer.class);
		MockProducer producer2 = ctx.getBean("producer2", MockProducer.class);
		assertThat(producer1.transactionCommitted()).isTrue();
		assertThat(producer1.commitCount()).isEqualTo(1);
		assertThat(producer2.transactionCommitted()).isTrue();
		assertThat(producer2.commitCount()).isEqualTo(1);
		assertThat(producer1.history()).containsExactly(new ProducerRecord("foo", "bar"),
				new ProducerRecord("baz", "qux"));
		assertThat(producer2.history()).containsExactly(new ProducerRecord("fiz", "buz"));
		ctx.close();
	}

	@Test
	public void testDefaultProducerIdempotentConfig() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setTransactionIdPrefix("my.transaction.");
		pf.destroy();
		assertThat(pf.getConfigurationProperties().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
	}

	@Test
	public void testOverrideProducerIdempotentConfig() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setTransactionIdPrefix("my.transaction.");
		pf.destroy();
		assertThat(pf.getConfigurationProperties().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(false);
	}

	@Test
	public void testNoTx() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		pf.setTransactionIdPrefix("my.transaction.");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		assertThatIllegalStateException()
				.isThrownBy(() -> template.send("foo", "bar"))
				.withMessageContaining("No transaction is in process;");
	}

	@Test
	public void testTransactionSynchronization() {
		StringSerializer ss = new StringSerializer();
		MockProducer<String, String> producer = spy(new MockProducer<>(false, null, ss, ss));
		producer.initTransactions();

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		ResourcelessTransactionManager tm = spy(new ResourcelessTransactionManager());

		new TransactionTemplate(tm)
				.executeWithoutResult(s -> template.sendDefault("foo", "bar"));

		assertThat(producer.history()).containsExactly(new ProducerRecord<>(STRING_KEY_TOPIC, "foo", "bar"));
		assertThat(producer.transactionCommitted()).isTrue();
		assertThat(producer.closed()).isTrue();

		InOrder inOrder = inOrder(producer, tm);
		inOrder.verify(tm).doBegin(any(), any());
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).send(any(), any());
		inOrder.verify(tm).doCommit(any());
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close(any());
	}

	@Test
	public void testTransactionSynchronizationExceptionOnCommit() {
		StringSerializer ss = new StringSerializer();
		MockProducer<String, String> producer = new MockProducer<>(false, null, ss, ss);
		producer.initTransactions();

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		ResourcelessTransactionManager tm = new ResourcelessTransactionManager();

		assertThatExceptionOfType(ProducerFencedException.class).isThrownBy(() ->
				new TransactionTemplate(tm)
						.executeWithoutResult(s -> {
							template.sendDefault("foo", "bar");

							// Mark the mock producer as fenced so it throws when committing the transaction
							producer.fenceProducer();
						}));

		assertThat(producer.transactionCommitted()).isFalse();
		assertThat(producer.closed()).isTrue();
	}

	@Test
	public void testDeadLetterPublisherWhileTransactionActive() {
		@SuppressWarnings("unchecked")
		Producer<Object, Object> producer1 = mock(Producer.class);
		given(producer1.send(any(), any())).willReturn(new CompletableFuture<>());
		@SuppressWarnings("unchecked")
		Producer<Object, Object> producer2 = mock(Producer.class);
		given(producer2.send(any(), any())).willReturn(new CompletableFuture<>());
		producer1.initTransactions();

		@SuppressWarnings("unchecked")
		ProducerFactory<Object, Object> pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		given(pf.createProducer(isNull())).willReturn(producer1).willReturn(producer2);

		KafkaOperations<Object, Object> template = spy(new KafkaTemplate<>(pf));
		((KafkaTemplate<Object, Object>) template).setDefaultTopic(STRING_KEY_TOPIC);

		KafkaTransactionManager<Object, Object> tm = new KafkaTransactionManager<>(pf);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
		recoverer.setFailIfSendResultIsError(false);

		new TransactionTemplate(tm)
				.executeWithoutResult(s ->
						recoverer.accept(
								new ConsumerRecord<>(STRING_KEY_TOPIC, 0, 0L, "key", "foo"),
								new RuntimeException("foo")));

		verify(producer1).beginTransaction();

		verify(producer1).commitTransaction();
		verify(producer1).close(any());
		verify(producer2, never()).beginTransaction();
		verify(template, never()).executeInTransaction(any());
	}

	@Test
	public void testNoAbortAfterCommitFailure() {
		MockProducer<String, String> producer = spy(new MockProducer<>());
		producer.initTransactions();

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		assertThatExceptionOfType(ProducerFencedException.class)
				.isThrownBy(() ->
						template.executeInTransaction(t -> {
							producer.fenceProducer();
							return null;
						}));

		assertThat(producer.transactionCommitted()).isFalse();
		assertThat(producer.transactionAborted()).isFalse();
		assertThat(producer.closed()).isTrue();
		verify(producer, never()).abortTransaction();
		verify(producer).close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
	}

	@Test
	public void testQuickCloseAfterCommitTimeout() {
		@SuppressWarnings("unchecked")
		Producer<String, String> producer = mock(Producer.class);

		DefaultKafkaProducerFactory<String, String> pf =
				new DefaultKafkaProducerFactory<String, String>(Collections.emptyMap()) {

					@SuppressWarnings({"rawtypes", "unchecked"})
					@Override
					public Producer<String, String> createProducer(@Nullable String txIdPrefixArg) {
						CloseSafeProducer<String, String> closeSafeProducer = new CloseSafeProducer<>(producer,
								(prod, timeout) -> {
									prod.closeDelegate(timeout);
									return true;
								},
								Duration.ofSeconds(1), "factory", 0);
						return closeSafeProducer;
					}

				};
		pf.setTransactionIdPrefix("foo");

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		willThrow(new TimeoutException()).given(producer).commitTransaction();
		assertThatExceptionOfType(TimeoutException.class)
				.isThrownBy(() ->
						template.executeInTransaction(t -> {
							return null;
						}));
		verify(producer, never()).abortTransaction();
		verify(producer).close(Duration.ofMillis(0));
	}

	@Test
	void testNormalCloseAfterCommitCacheFull() {
		@SuppressWarnings("unchecked")
		Producer<String, String> producer = mock(Producer.class);

		DefaultKafkaProducerFactory<String, String> pf =
				new DefaultKafkaProducerFactory<String, String>(Collections.emptyMap()) {

					@SuppressWarnings("unchecked")
					@Override
					public Producer<String, String> createProducer(@Nullable String txIdPrefixArg) {
						BlockingQueue<CloseSafeProducer<String, String>> cache = new LinkedBlockingDeque<>(1);
						try {
							cache.put(new CloseSafeProducer<>(mock(Producer.class), this::removeProducer,
									Duration.ofSeconds(1), "factory", 0));
						}
						catch (@SuppressWarnings("unused") InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						KafkaTestUtils.getPropertyValue(this, "cache", Map.class).put("foo", cache);
						CloseSafeProducer<String, String> closeSafeProducer = new CloseSafeProducer<>(producer,
								this::cacheReturner, "foo", "1", Duration.ofSeconds(1), "factory", 0);
						return closeSafeProducer;
					}

				};
		pf.setTransactionIdPrefix("foo");

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		template.executeInTransaction(t -> {
			return null;
		});
		verify(producer).close(ProducerFactoryUtils.DEFAULT_CLOSE_TIMEOUT);
	}

	@Test
	public void testFencedOnBegin() {
		MockProducer<String, String> producer = spy(new MockProducer<>());
		producer.initTransactions();
		producer.fenceProducer();

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		assertThatExceptionOfType(ProducerFencedException.class)
				.isThrownBy(() -> template.executeInTransaction(t -> null));

		assertThat(producer.transactionCommitted()).isFalse();
		assertThat(producer.transactionAborted()).isFalse();
		assertThat(producer.closed()).isTrue();
		verify(producer, never()).commitTransaction();
	}

	@Test
	public void testAbort() {
		MockProducer<String, String> producer = spy(new MockProducer<>());
		producer.initTransactions();

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() ->
						template.executeInTransaction(t -> {
							throw new RuntimeException("foo");
						}))
				.withMessage("foo");

		assertThat(producer.transactionCommitted()).isFalse();
		assertThat(producer.transactionAborted()).isTrue();
		assertThat(producer.closed()).isTrue();
		verify(producer, never()).commitTransaction();
	}

	@Test
	public void abortFiledOriginalExceptionRethrown() {
		MockProducer<String, String> producer = spy(new MockProducer<>());
		producer.initTransactions();
		producer.abortTransactionException = new RuntimeException("abort failed");

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() ->
						template.executeInTransaction(t -> {
							throw new RuntimeException("intentional");
						}))
				.withMessage("intentional")
				.withStackTraceContaining("abort failed");

		assertThat(producer.transactionCommitted()).isFalse();
		assertThat(producer.transactionAborted()).isFalse();
		assertThat(producer.closed()).isTrue();
		verify(producer, never()).commitTransaction();
	}

	@Test
	public void testExecuteInTransactionNewInnerTx() {
		@SuppressWarnings("unchecked")
		Producer<Object, Object> producer1 = mock(Producer.class);
		given(producer1.send(any(), any())).willReturn(new CompletableFuture<>());
		@SuppressWarnings("unchecked")
		Producer<Object, Object> producer2 = mock(Producer.class);
		given(producer2.send(any(), any())).willReturn(new CompletableFuture<>());
		producer1.initTransactions();
		AtomicBoolean first = new AtomicBoolean(true);

		DefaultKafkaProducerFactory<Object, Object> pf =
				new DefaultKafkaProducerFactory<Object, Object>(
						Collections.emptyMap()) {

					@Override
					protected Producer<Object, Object> createTransactionalProducer(String txIdPrefix) {
						return first.getAndSet(false) ? producer1 : producer2;
					}

				};
		pf.setTransactionIdPrefix("tx.");

		KafkaTemplate<Object, Object> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		KafkaTransactionManager<Object, Object> tm = new KafkaTransactionManager<>(pf);

		new TransactionTemplate(tm)
				.executeWithoutResult(s ->
						template.executeInTransaction(t -> {
							template.sendDefault("foo", "bar");
							return null;
						}));

		InOrder inOrder = inOrder(producer1, producer2);
		inOrder.verify(producer1).beginTransaction();
		inOrder.verify(producer2).beginTransaction();
		inOrder.verify(producer2).commitTransaction();
		inOrder.verify(producer2).close(any());
		inOrder.verify(producer1).commitTransaction();
		inOrder.verify(producer1).close(any());
	}

	@Test
	void testNonTxWithTx() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(this.embeddedKafka);
		senderProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx.");
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
		template.executeInTransaction(tmp -> tmp.execute(prod -> {
			assertThat(KafkaTestUtils.getPropertyValue(prod, "delegate.transactionManager.transactionalId"))
					.isEqualTo("tx.0");
			return null;
		}));
		assertThatIllegalStateException().isThrownBy(() -> template.execute(prod -> {
			return null;
		}));
		template.setAllowNonTransactional(true);
		template.execute(prod -> {
			assertThat(KafkaTestUtils.getPropertyValue(prod, "delegate.transactionManager.transactionalId")).isNull();
			return null;
		});
		pf.destroy();
	}

	@Test
	void syncCommitFails() {
		DummyTM tm = new DummyTM();
		MockProducer<String, String> producer =
				new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
		producer.initTransactions();
		producer.commitTransactionException = new IllegalStateException();

		ProducerFactory<String, String> pf = new MockProducerFactory<>((tx, id) -> producer, null);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() ->
				new TransactionTemplate(tm).execute(status -> template.sendDefault("foo")));

		assertThat(tm.committed).isTrue();
	}

	@Configuration
	@EnableTransactionManagement
	public static class DeclarativeConfig {

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public Producer producer1() {
			Producer mock = mock(Producer.class);
			given(mock.send(any(), any())).willReturn(new CompletableFuture<>());
			return mock;
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public Producer producer2() {
			Producer mock = mock(Producer.class);
			given(mock.send(any(), any())).willReturn(new CompletableFuture<>());
			return mock;
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public ProducerFactory pf() {
			ProducerFactory pf = mock(ProducerFactory.class);
			given(pf.transactionCapable()).willReturn(true);
			given(pf.createProducer(isNull())).willReturn(producer1());
			given(pf.createProducer(anyString())).willReturn(producer2());
			return pf;
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public KafkaTransactionManager transactionManager() {
			return new KafkaTransactionManager(pf());
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public KafkaTransactionManager customTM() {
			KafkaTransactionManager tm = new KafkaTransactionManager(pf());
			tm.setTransactionIdPrefix("custom.tx.prefix.");
			return tm;
		}

		@SuppressWarnings({"unchecked"})
		@Bean
		public KafkaTemplate<String, String> template() {
			return new KafkaTemplate<>(pf());
		}

		@Bean
		public Tx1 tx1() {
			return new Tx1(template(), tx2());
		}

		@Bean
		public Tx2 tx2() {
			return new Tx2(template());
		}

	}

	@Configuration(proxyBeanMethods = false)
	@EnableTransactionManagement
	public static class DeclarativeConfigWithMockProducer {

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public Producer producer1() {
			MockProducer mockProducer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
			mockProducer.initTransactions();
			return mockProducer;
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public Producer producer2() {
			MockProducer mockProducer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
			mockProducer.initTransactions();
			return mockProducer;
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public ProducerFactory pf(@Qualifier("producer1") Producer producer1, @Qualifier("producer2") Producer producer2) {
			return new MockProducerFactory((tx, id) -> id.equals("default") ? producer1 : producer2, "default");
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public KafkaTransactionManager transactionManager(ProducerFactory pf) {
			return new KafkaTransactionManager(pf);
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public KafkaTransactionManager customTM(ProducerFactory pf) {
			KafkaTransactionManager tm = new KafkaTransactionManager(pf);
			tm.setTransactionIdPrefix("custom.tx.prefix.");
			return tm;
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		public KafkaTemplate<String, String> template(ProducerFactory pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		public Tx1 tx1(KafkaTemplate<String, String> template, Tx2 tx2) {
			return new Tx1(template, tx2);
		}

		@Bean
		public Tx2 tx2(KafkaTemplate<String, String> template) {
			return new Tx2(template);
		}

	}

	public static class Tx1 {

		@SuppressWarnings("rawtypes")
		private final KafkaTemplate template;

		private final Tx2 tx2;

		volatile String txId;

		@SuppressWarnings("rawtypes")
		public Tx1(KafkaTemplate template, Tx2 tx2) {
			this.template = template;
			this.tx2 = tx2;
		}

		@SuppressWarnings("unchecked")
		@Transactional("transactionManager")
		public void txMethod() {
			template.send("foo", "bar");
			template.send("baz", "qux");
			this.tx2.anotherTxMethod();
		}

	}

	public static class Tx2 {

		@SuppressWarnings("rawtypes")
		private final KafkaTemplate template;

		volatile String txId;

		@SuppressWarnings("rawtypes")
		public Tx2(KafkaTemplate template) {
			this.template = template;
		}

		@SuppressWarnings("unchecked")
		@Transactional(propagation = Propagation.REQUIRES_NEW, transactionManager = "customTM")
		public void anotherTxMethod() {
			template.send("fiz", "buz");
		}

	}

	@SuppressWarnings("serial")
	private static final class DummyTM extends AbstractPlatformTransactionManager {

		boolean committed;

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
			this.committed = true;
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

}
