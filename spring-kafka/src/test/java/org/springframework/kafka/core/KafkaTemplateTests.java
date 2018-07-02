/*
 * Copyright 2016-2018 the original author or authors.
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
import static org.mockito.Mockito.mock;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;
import static org.springframework.kafka.test.assertj.KafkaConditions.timestamp;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.CompositeProducerListener;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Igor Stepanov
 * @author Biju Kunjummen
 * @author Endika Gutiérrez
 */
public class KafkaTemplateTests {

	private static final String INT_KEY_TOPIC = "intKeyTopic";

	private static final String STRING_KEY_TOPIC = "stringKeyTopic";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, INT_KEY_TOPIC, STRING_KEY_TOPIC);

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<Integer, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils
				.consumerProps("KafkaTemplatetests" + UUID.randomUUID().toString(), "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, INT_KEY_TOPIC);
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testTemplate() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);

		template.setDefaultTopic(INT_KEY_TOPIC);

		template.sendDefault("foo");
		assertThat(KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC)).has(value("foo"));

		template.sendDefault(0, 2, "bar");
		ConsumerRecord<Integer, String> received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("bar"));

		template.send(INT_KEY_TOPIC, 0, 2, "baz");
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("baz"));

		template.send(INT_KEY_TOPIC, 0, null, "qux");
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(key((Integer) null));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("qux"));

		template.send(MessageBuilder.withPayload("fiz")
				.setHeader(KafkaHeaders.TOPIC, INT_KEY_TOPIC)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("fiz"));

		template.send(MessageBuilder.withPayload("buz")
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("buz"));

		Map<MetricName, ? extends Metric> metrics = template.execute(Producer::metrics);
		assertThat(metrics).isNotNull();
		metrics = template.metrics();
		assertThat(metrics).isNotNull();
		List<PartitionInfo> partitions = template.partitionsFor(INT_KEY_TOPIC);
		assertThat(partitions).isNotNull();
		assertThat(partitions).hasSize(2);
		pf.destroy();
	}

	@Test
	public void testTemplateWithTimestamps() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);

		template.setDefaultTopic(INT_KEY_TOPIC);

		template.sendDefault(0, 1487694048607L, null, "foo-ts1");
		ConsumerRecord<Integer, String> r1 = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(r1).has(value("foo-ts1"));
		assertThat(r1).has(timestamp(1487694048607L));

		template.send(INT_KEY_TOPIC, 0, 1487694048610L, null, "foo-ts2");
		ConsumerRecord<Integer, String> r2 = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(r2).has(value("foo-ts2"));
		assertThat(r2).has(timestamp(1487694048610L));

		Map<MetricName, ? extends Metric> metrics = template.execute(Producer::metrics);
		assertThat(metrics).isNotNull();
		metrics = template.metrics();
		assertThat(metrics).isNotNull();
		List<PartitionInfo> partitions = template.partitionsFor(INT_KEY_TOPIC);
		assertThat(partitions).isNotNull();
		assertThat(partitions).hasSize(2);
		pf.destroy();
	}

	@Test
	public void testWithMessage() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);

		Message<String> message1 = MessageBuilder.withPayload("foo-message")
				.setHeader(KafkaHeaders.TOPIC, INT_KEY_TOPIC)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader("foo", "bar")
				.setHeader(KafkaHeaders.RECEIVED_TOPIC, "dummy")
				.build();

		template.send(message1);

		ConsumerRecord<Integer, String> r1 = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(r1).has(value("foo-message"));
		Iterator<Header> iterator = r1.headers().iterator();
		assertThat(iterator.hasNext()).isTrue();
		Header next = iterator.next();
		assertThat(next.key()).isEqualTo("foo");
		assertThat(new String(next.value())).isEqualTo("\"bar\"");
		assertThat(iterator.hasNext()).isTrue();
		next = iterator.next();
		assertThat(next.key()).isEqualTo(DefaultKafkaHeaderMapper.JSON_TYPES);
		assertThat(iterator.hasNext()).as("Expected no more headers").isFalse();

		Message<String> message2 = MessageBuilder.withPayload("foo-message-2")
				.setHeader(KafkaHeaders.TOPIC, INT_KEY_TOPIC)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.TIMESTAMP, 1487694048615L)
				.setHeader("foo", "bar")
				.build();

		template.send(message2);

		ConsumerRecord<Integer, String> r2 = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(r2).has(value("foo-message-2"));
		assertThat(r2).has(timestamp(1487694048615L));

		MessagingMessageConverter messageConverter = new MessagingMessageConverter();

		Acknowledgment ack = mock(Acknowledgment.class);
		Consumer<?, ?> mockConsumer = mock(Consumer.class);
		Message<?> recordToMessage = messageConverter.toMessage(r2, ack, mockConsumer, String.class);

		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.TIMESTAMP_TYPE)).isEqualTo("CREATE_TIME");
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP)).isEqualTo(1487694048615L);
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo(INT_KEY_TOPIC);
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isSameAs(ack);
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.CONSUMER)).isSameAs(mockConsumer);
		assertThat(recordToMessage.getHeaders().get("foo")).isEqualTo("bar");
		assertThat(recordToMessage.getPayload()).isEqualTo("foo-message-2");

		pf.destroy();
	}

	@Test
	public void withListener() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(INT_KEY_TOPIC);
		final CountDownLatch latch = new CountDownLatch(2);
		final List<ProducerRecord<Integer, String>> records = new ArrayList<>();
		final List<RecordMetadata> meta = new ArrayList<>();
		final AtomicInteger onErrorDelegateCalls = new AtomicInteger();
		class PL implements ProducerListener<Integer, String> {

			@Override
			public void onSuccess(ProducerRecord<Integer, String> record, RecordMetadata recordMetadata) {
				records.add(record);
				meta.add(recordMetadata);
				latch.countDown();
			}

			@Override
			public void onError(ProducerRecord<Integer, String> producerRecord, Exception exception) {
				assertThat(producerRecord).isNotNull();
				assertThat(exception).isNotNull();
				onErrorDelegateCalls.incrementAndGet();
			}

		}
		PL pl1 = new PL();
		PL pl2 = new PL();
		@SuppressWarnings("unchecked")
		CompositeProducerListener<Integer, String> cpl = new CompositeProducerListener<>(pl1, pl2);
		template.setProducerListener(cpl);
		template.sendDefault("foo");
		template.flush();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(records.get(0).value()).isEqualTo("foo");
		assertThat(records.get(1).value()).isEqualTo("foo");
		assertThat(meta.get(0).topic()).isEqualTo(INT_KEY_TOPIC);
		assertThat(meta.get(1).topic()).isEqualTo(INT_KEY_TOPIC);

		//Drain the topic
		KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		pf.destroy();
		cpl.onError(records.get(0), new RuntimeException("x"));
		assertThat(onErrorDelegateCalls.get()).isEqualTo(2);
	}

	@Test
	public void withProducerRecordListener() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(INT_KEY_TOPIC);
		final CountDownLatch latch = new CountDownLatch(1);
		template.setProducerListener(new ProducerListener<Integer, String>() {

			@Override
			public void onSuccess(ProducerRecord<Integer, String> record, RecordMetadata recordMetadata) {
				latch.countDown();
			}

		});
		template.sendDefault("foo");
		template.flush();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		//Drain the topic
		KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		pf.destroy();
	}

	@Test
	public void testWithCallback() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(INT_KEY_TOPIC);
		ListenableFuture<SendResult<Integer, String>> future = template.sendDefault("foo");
		template.flush();
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<SendResult<Integer, String>> theResult = new AtomicReference<>();
		future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				theResult.set(result);
				latch.countDown();
			}

			@Override
			public void onFailure(Throwable ex) {
			}

		});
		assertThat(KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC)).has(value("foo"));
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		pf.createProducer().close();
	}

	@Test
	public void testTemplateDisambiguation() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testTString", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, STRING_KEY_TOPIC);
		template.sendDefault("foo", "bar");
		template.flush();
		ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, STRING_KEY_TOPIC);
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("foo"), value("bar")));
		consumer.close();
		pf.createProducer().close();
		pf.destroy();
	}

}
