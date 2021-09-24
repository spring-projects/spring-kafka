/*
 * Copyright 2016-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.keyValue;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;
import static org.springframework.kafka.test.assertj.KafkaConditions.timestamp;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.CompositeProducerListener;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Igor Stepanov
 * @author Biju Kunjummen
 * @author Endika Guti?rrez
 */
@EmbeddedKafka(topics = { KafkaTemplateTests.INT_KEY_TOPIC, KafkaTemplateTests.STRING_KEY_TOPIC })
public class KafkaTemplateTests {

	public static final String INT_KEY_TOPIC = "intKeyTopic";

	public static final String STRING_KEY_TOPIC = "stringKeyTopic";

	private static EmbeddedKafkaBroker embeddedKafka;

	private static Consumer<Integer, String> consumer;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils
				.consumerProps("KafkaTemplatetests" + UUID.randomUUID().toString(), "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, INT_KEY_TOPIC);
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void testTemplate() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		AtomicReference<Producer<Integer, String>> wrapped = new AtomicReference<>();
		pf.addPostProcessor(prod -> {
			ProxyFactory prox = new ProxyFactory();
			prox.setTarget(prod);
			@SuppressWarnings("unchecked")
			Producer<Integer, String> proxy =  (Producer<Integer, String>) prox.getProxy();
			wrapped.set(proxy);
			return proxy;
		});
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);

		template.setDefaultTopic(INT_KEY_TOPIC);

		template.sendDefault("foo");
		assertThat(KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC)).has(value("foo"));

		template.sendDefault(0, 2, "bar");
		ConsumerRecord<Integer, String> received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(allOf(keyValue(2, "bar"), partition(0)));

		template.send(INT_KEY_TOPIC, 0, 2, "baz");
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(allOf(keyValue(2, "baz"), partition(0)));

		template.send(INT_KEY_TOPIC, 1, null, "qux");
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(allOf(keyValue(null, "qux"), partition(1)));

		template.send(MessageBuilder.withPayload("fiz")
				.setHeader(KafkaHeaders.TOPIC, INT_KEY_TOPIC)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(allOf(keyValue(2, "fiz"), partition(0)));

		template.send(MessageBuilder.withPayload("buz")
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		received = KafkaTestUtils.getSingleRecord(consumer, INT_KEY_TOPIC);
		assertThat(received).has(allOf(keyValue(2, "buz"), partition(1)));

		Map<MetricName, ? extends Metric> metrics = template.execute(Producer::metrics);
		assertThat(metrics).isNotNull();
		metrics = template.metrics();
		assertThat(metrics).isNotNull();
		List<PartitionInfo> partitions = template.partitionsFor(INT_KEY_TOPIC);
		assertThat(partitions).isNotNull();
		assertThat(partitions).hasSize(2);
		assertThat(KafkaTestUtils.getPropertyValue(pf.createProducer(), "delegate")).isSameAs(wrapped.get());
		template.setConsumerFactory(
				new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("xx", "false", embeddedKafka)));
		ConsumerRecord<Integer, String> receive = template.receive(INT_KEY_TOPIC, 1, received.offset());
		assertThat(receive).has(allOf(keyValue(2, "buz"), partition(1)))
				.extracting(rec -> rec.offset())
				.isEqualTo(received.offset());
		ConsumerRecords<Integer, String> records = template.receive(List.of(
				new TopicPartitionOffset(INT_KEY_TOPIC, 1, 1L),
				new TopicPartitionOffset(INT_KEY_TOPIC, 0, 1L),
				new TopicPartitionOffset(INT_KEY_TOPIC, 0, 0L),
				new TopicPartitionOffset(INT_KEY_TOPIC, 1, 0L)));
		assertThat(records.count()).isEqualTo(4);
		Set<TopicPartition> partitions2 = records.partitions();
		assertThat(partitions2).containsExactly(
				new TopicPartition(INT_KEY_TOPIC, 1),
				new TopicPartition(INT_KEY_TOPIC, 0));
		assertThat(records.records(new TopicPartition(INT_KEY_TOPIC, 1)))
			.extracting(rec -> rec.offset())
			.containsExactly(1L, 0L);
		assertThat(records.records(new TopicPartition(INT_KEY_TOPIC, 0)))
		.extracting(rec -> rec.offset())
		.containsExactly(1L, 0L);
		pf.destroy();
	}

	@Test
	void testTemplateWithTimestamps() {
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
	void testWithMessage() {
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
		assertThat(new String(next.value())).isEqualTo("bar");
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
		KafkaUtils.setConsumerGroupId("test.group.id");
		Message<?> recordToMessage = messageConverter.toMessage(r2, ack, mockConsumer, String.class);

		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.TIMESTAMP_TYPE)).isEqualTo("CREATE_TIME");
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP)).isEqualTo(1487694048615L);
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo(INT_KEY_TOPIC);
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isSameAs(ack);
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.CONSUMER)).isSameAs(mockConsumer);
		assertThat(recordToMessage.getHeaders().get("foo")).isEqualTo("bar");
		assertThat(recordToMessage.getPayload()).isEqualTo("foo-message-2");
		assertThat(recordToMessage.getHeaders().get(KafkaHeaders.GROUP_ID)).isEqualTo("test.group.id");
		KafkaUtils.clearConsumerGroupId();
		pf.destroy();
	}

	@Test
	void withListener() throws Exception {
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
			public void onError(ProducerRecord<Integer, String> producerRecord, RecordMetadata metadata,
					Exception exception) {

				assertThat(producerRecord).isNotNull();
				assertThat(exception).isNotNull();
				onErrorDelegateCalls.incrementAndGet();
			}

		}
		PL pl1 = new PL();
		PL pl2 = new PL();
		CompositeProducerListener<Integer, String> cpl = new CompositeProducerListener<>(new PL[] { pl1, pl2 });
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
		cpl.onError(records.get(0), new RecordMetadata(new TopicPartition(INT_KEY_TOPIC, -1), 0L, 0L, 0L, 0L, 0, 0),
				new RuntimeException("x"));
		assertThat(onErrorDelegateCalls.get()).isEqualTo(2);
	}

	@Test
	void withProducerRecordListener() throws Exception {
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
	void testWithCallback() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
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
		pf.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test
	void testWithCallbackFailure() throws Exception {
		Producer<Integer, String> producer = mock(Producer.class);
		willAnswer(inv -> {
			Callback callback = inv.getArgument(1);
			callback.onCompletion(null, new RuntimeException("test"));
			return new SettableListenableFuture<RecordMetadata>();
		}).given(producer).send(any(), any());
		ProducerFactory<Integer, String> pf = mock(ProducerFactory.class);
		given(pf.createProducer()).willReturn(producer);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		ListenableFuture<SendResult<Integer, String>> future = template.send("foo", 1, "bar");
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<SendResult<Integer, String>> theResult = new AtomicReference<>();
		AtomicReference<String> value = new AtomicReference<>();
		future.addCallback(new KafkaSendCallback<Integer, String>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
			}

			@Override
			public void onFailure(KafkaProducerException ex) {
				ProducerRecord<Integer, String> failed = ex.getFailedProducerRecord();
				value.set(failed.value());
				latch.countDown();
			}

		});
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(value.get()).isEqualTo("bar");
	}

	@SuppressWarnings("unchecked")
	@Test
	void testWithCallbackFailureFunctional() throws Exception {
		Producer<Integer, String> producer = mock(Producer.class);
		willAnswer(inv -> {
			Callback callback = inv.getArgument(1);
			callback.onCompletion(null, new RuntimeException("test"));
			return new SettableListenableFuture<RecordMetadata>();
		}).given(producer).send(any(), any());
		ProducerFactory<Integer, String> pf = mock(ProducerFactory.class);
		given(pf.createProducer()).willReturn(producer);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		ListenableFuture<SendResult<Integer, String>> future = template.send("foo", 1, "bar");
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<SendResult<Integer, String>> theResult = new AtomicReference<>();
		AtomicReference<String> value = new AtomicReference<>();
		future.addCallback(result -> { }, (KafkaFailureCallback<Integer, String>) ex -> {
			ProducerRecord<Integer, String> failed = ex.getFailedProducerRecord();
			value.set(failed.value());
			latch.countDown();
		});
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(value.get()).isEqualTo("bar");
	}

	@Test
	void testTemplateDisambiguation() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setKeySerializer(new StringSerializer());
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(STRING_KEY_TOPIC);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testTString", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		cf.setKeyDeserializer(new StringDeserializer());
		Consumer<String, String> localConsumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(localConsumer, STRING_KEY_TOPIC);
		template.sendDefault("foo", "bar");
		template.flush();
		ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(localConsumer, STRING_KEY_TOPIC);
		assertThat(record).has(Assertions.<ConsumerRecord<String, String>>allOf(key("foo"), value("bar")));
		localConsumer.close();
		pf.createProducer().close();
		pf.destroy();
	}

	@Test
	void testConfigOverrides() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setPhysicalCloseTimeout(6);
		pf.setProducerPerConsumerPartition(false);
		pf.setProducerPerThread(true);
		Map<String, Object> overrides = new HashMap<>();
		overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		overrides.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "TX");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true, overrides);
		assertThat(template.getProducerFactory().getConfigurationProperties()
				.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
		assertThat(template.getProducerFactory().getPhysicalCloseTimeout()).isEqualTo(Duration.ofSeconds(6));
		assertThat(template.getProducerFactory().isProducerPerConsumerPartition()).isFalse();
		assertThat(template.getProducerFactory().isProducerPerThread()).isTrue();
		assertThat(template.isTransactional()).isTrue();
	}

	@Test
	void testConfigOverridesWithSerializers() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		Supplier<Serializer<String>> keySerializer = () -> null;
		Supplier<Serializer<String>> valueSerializer = () -> null;
		DefaultKafkaProducerFactory<String, String> pf =
				new DefaultKafkaProducerFactory<>(senderProps, keySerializer, valueSerializer);
		Map<String, Object> overrides = new HashMap<>();
		overrides.put(ProducerConfig.CLIENT_ID_CONFIG, "foo");
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true, overrides);
		assertThat(template.getProducerFactory().getConfigurationProperties()
				.get(ProducerConfig.CLIENT_ID_CONFIG)).isEqualTo("foo");
		assertThat(template.getProducerFactory().getKeySerializerSupplier()).isSameAs(keySerializer);
		assertThat(template.getProducerFactory().getValueSerializerSupplier()).isSameAs(valueSerializer);
	}

	@Test
	void testFutureFailureOnSend() {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);

		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
			template.send("missing.topic", "foo"))
				.withCauseExactlyInstanceOf(TimeoutException.class);
		pf.destroy();
	}

}
