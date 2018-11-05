/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.InOrder;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.support.transaction.ResourcelessTransactionManager;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
public class KafkaTemplateTransactionTests {

	private static final String STRING_KEY_TOPIC = "stringKeyTopic";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(3, true, STRING_KEY_TOPIC);

	@Test
	public void testLocalTransaction() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		pf.setTransactionIdPrefix("my.transaction.");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testTxString", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, STRING_KEY_TOPIC);
		template.executeInTransaction(t -> {
			t.sendDefault("foo", "bar");
			t.sendDefault("baz", "qux");
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
		consumer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", BlockingQueue.class).size()).isEqualTo(1);
		pf.destroy();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", BlockingQueue.class).size()).isEqualTo(0);
	}

	@Test
	public void testGlobalTransaction() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		pf.setTransactionIdPrefix("my.transaction.");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testTxString", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, STRING_KEY_TOPIC);
		KafkaTransactionManager<String, String> tm = new KafkaTransactionManager<>(pf);
		tm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
		new TransactionTemplate(tm).execute(s -> {
			template.sendDefault("foo", "bar");
			template.sendDefault("baz", "qux");
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
		consumer.close();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", BlockingQueue.class).size()).isEqualTo(1);
		pf.destroy();
		assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", BlockingQueue.class).size()).isEqualTo(0);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testDeclarative() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(DeclarativeConfig.class);
		Tx1 tx1 = ctx.getBean(Tx1.class);
		tx1.txMethod();
		ProducerFactory producerFactory = ctx.getBean(ProducerFactory.class);
		verify(producerFactory, times(2)).createProducer();
		Producer producer1 = ctx.getBean("producer1", Producer.class);
		Producer producer2 = ctx.getBean("producer1", Producer.class);
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

	@Test
	public void testNoTx() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		pf.setTransactionIdPrefix("my.transaction.");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		assertThatThrownBy(() -> template.send("foo", "bar"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("No transaction is in process;");
	}

	@Test
	public void testTransactionSynchronization() {
		MockProducer<String, String> producer = new MockProducer<>();
		producer.initTransactions();

		@SuppressWarnings("unchecked")
		ProducerFactory<String, String> pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		given(pf.createProducer()).willReturn(producer);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		ResourcelessTransactionManager tm = new ResourcelessTransactionManager();

		new TransactionTemplate(tm).execute(s -> {
			template.sendDefault("foo", "bar");
			return null;
		});

		assertThat(producer.history()).containsExactly(new ProducerRecord<>(STRING_KEY_TOPIC, "foo", "bar"));
		assertThat(producer.transactionCommitted()).isTrue();
		assertThat(producer.closed()).isTrue();
	}

	@Test
	public void testTransactionSynchronizationExceptionOnCommit() {
		MockProducer<String, String> producer = new MockProducer<>();
		producer.initTransactions();

		@SuppressWarnings("unchecked")
		ProducerFactory<String, String> pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		given(pf.createProducer()).willReturn(producer);

		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		ResourcelessTransactionManager tm = new ResourcelessTransactionManager();

		new TransactionTemplate(tm).execute(s -> {
			template.sendDefault("foo", "bar");

			// Mark the mock producer as fenced so it throws when committing the transaction
			producer.fenceProducer();
			return null;
		});

		assertThat(producer.transactionCommitted()).isFalse();
		assertThat(producer.closed()).isTrue();
	}

	@Test
	public void testExcecuteInTransactionNewInnerTx() {
		@SuppressWarnings("unchecked")
		Producer<Object, Object> producer1 = mock(Producer.class);
		@SuppressWarnings("unchecked")
		Producer<Object, Object> producer2 = mock(Producer.class);
		producer1.initTransactions();
		AtomicBoolean first = new AtomicBoolean(true);

		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<Object, Object>(
				Collections.emptyMap()) {

			@Override
			protected Producer<Object, Object> createTransactionalProducer() {
				return first.getAndSet(false) ? producer1 : producer2;
			}

			@Override
			Producer<Object, Object> createTransactionalProducerForPartition() {
				return createTransactionalProducer();
			}

		};
		pf.setTransactionIdPrefix("tx.");

		KafkaTemplate<Object, Object> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(STRING_KEY_TOPIC);

		KafkaTransactionManager<Object, Object> tm = new KafkaTransactionManager<>(pf);

		try {
			TransactionSupport.setTransactionIdSuffix("testExcecuteInTransactionNewInnerTx");
			new TransactionTemplate(tm).execute(s -> {
				return template.executeInTransaction(t -> {
					template.sendDefault("foo", "bar");
					return null;
				});
			});

			InOrder inOrder = inOrder(producer1, producer2);
			inOrder.verify(producer1).beginTransaction();
			inOrder.verify(producer2).beginTransaction();
			inOrder.verify(producer2).commitTransaction();
			inOrder.verify(producer2).close();
			inOrder.verify(producer1).commitTransaction();
			inOrder.verify(producer1).close();
		}
		finally {
			TransactionSupport.clearTransactionIdSuffix();
		}
	}

	@Configuration
	@EnableTransactionManagement
	public static class DeclarativeConfig {

		@SuppressWarnings("rawtypes")
		@Bean
		public ProducerFactory pf() {
			ProducerFactory pf = mock(ProducerFactory.class);
			given(pf.transactionCapable()).willReturn(true);
			given(pf.createProducer()).willReturn(producer1(), producer2());
			return pf;
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public Producer producer1() {
			return mock(Producer.class);
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public Producer producer2() {
			return producer1();
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public KafkaTransactionManager transactionManager() {
			return new KafkaTransactionManager(pf());
		}

		@SuppressWarnings({ "unchecked" })
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

	public static class Tx1 {

		@SuppressWarnings("rawtypes")
		private final KafkaTemplate template;

		private final Tx2 tx2;

		@SuppressWarnings("rawtypes")
		public Tx1(KafkaTemplate template, Tx2 tx2) {
			this.template = template;
			this.tx2 = tx2;
		}

		@SuppressWarnings("unchecked")
		@Transactional
		public void txMethod() {
			template.send("foo", "bar");
			template.send("baz", "qux");
			this.tx2.anotherTxMethod();
		}

	}

	public static class Tx2 {

		@SuppressWarnings("rawtypes")
		private final KafkaTemplate template;

		@SuppressWarnings("rawtypes")
		public Tx2(KafkaTemplate template) {
			this.template = template;
		}

		@SuppressWarnings("unchecked")
		@Transactional(propagation = Propagation.REQUIRES_NEW)
		public void anotherTxMethod() {
			template.send("fiz", "buz");
		}

	}

}
