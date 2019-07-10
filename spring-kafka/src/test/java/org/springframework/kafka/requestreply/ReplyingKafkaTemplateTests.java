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

package org.springframework.kafka.requestreply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.adapter.ReplyHeadersConfigurer;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Gary Russell
 * @since 2.1.3
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class ReplyingKafkaTemplateTests {

	private static final String A_REPLY = "aReply";

	private static final String A_REQUEST = "aRequest";

	private static final String B_REPLY = "bReply";

	private static final String B_REQUEST = "bRequest";

	private static final String C_REPLY = "cReply";

	private static final String C_REQUEST = "cRequest";

	private static final String D_REPLY = "dReply";

	private static final String D_REQUEST = "dRequest";

	private static final String E_REPLY = "eReply";

	private static final String E_REQUEST = "eRequest";

	private static final String F_REPLY = "fReply";

	private static final String F_REQUEST = "fRequest";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, 5, A_REQUEST, A_REPLY,
			B_REQUEST, B_REPLY, C_REQUEST, C_REPLY, D_REQUEST, D_REPLY, E_REQUEST, E_REPLY, F_REQUEST, F_REPLY);

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Rule
	public TestName testName = new TestName();

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Test
	public void testGood() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setReplyTimeout(30_000);
			Headers headers = new RecordHeaders();
			headers.add("baz", "buz".getBytes());
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, null, null, null, "foo", headers);
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
			Map<String, Object> receivedHeaders = new HashMap<>();
			new DefaultKafkaHeaderMapper().toHeaders(consumerRecord.headers(), receivedHeaders);
			assertThat(receivedHeaders).containsKey("baz");
			assertThat(receivedHeaders).hasSize(2);
			assertThat(KafkaTestUtils.getPropertyValue(
					this.registry.getListenerContainer(A_REQUEST), "containerProperties.missingTopicsFatal",
					Boolean.class))
				.isFalse();
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testMultiListenerMessageReturn() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(C_REPLY);
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(C_REQUEST, "foo");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, C_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodDefaultReplyHeaders() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(
				new TopicPartitionOffset(A_REPLY, 3));
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, "bar");
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("BAR");
			assertThat(consumerRecord.partition()).isEqualTo(3);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodSamePartition() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, 2, null, "baz");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, A_REPLY.getBytes()));
			record.headers()
					.add(new RecordHeader(KafkaHeaders.REPLY_PARTITION, new byte[] { 0, 0, 0, 2 }));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("BAZ");
			assertThat(consumerRecord.partition()).isEqualTo(2);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testTimeout() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setReplyTimeout(1);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, "fiz");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, A_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			try {
				future.get(30, TimeUnit.SECONDS);
				fail("Expected Exception");
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			}
			catch (ExecutionException e) {
				assertThat(e)
					.hasCauseExactlyInstanceOf(KafkaReplyTimeoutException.class)
					.hasMessageContaining("Reply timed out");
			}
			assertThat(KafkaTestUtils.getPropertyValue(template, "futures", Map.class)).isEmpty();
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodWithSimpleMapper() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(B_REPLY);
		try {
			template.setReplyTimeout(30_000);
			Headers headers = new RecordHeaders();
			headers.add("baz", "buz".getBytes());
			ProducerRecord<Integer, String> record = new ProducerRecord<>(B_REQUEST, null, null, null, "qux", headers);
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, B_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("qUX");
			Map<String, Object> receivedHeaders = new HashMap<>();
			new DefaultKafkaHeaderMapper().toHeaders(consumerRecord.headers(), receivedHeaders);
			assertThat(receivedHeaders).containsKey("qux");
			assertThat(receivedHeaders).doesNotContainKey("baz");
			assertThat(receivedHeaders).hasSize(2);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testAggregateNormal() throws Exception {
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(D_REPLY, 0), 2);
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(D_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, Collection<ConsumerRecord<Integer, String>>> consumerRecord =
					future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value().size()).isEqualTo(2);
			Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecord.value().iterator();
			String value1 = iterator.next().value();
			assertThat(value1).isIn("fOO", "FOO");
			String value2 = iterator.next().value();
			assertThat(value2).isIn("fOO", "FOO");
			assertThat(value2).isNotSameAs(value1);
			assertThat(consumerRecord.topic()).isEqualTo(AggregatingReplyingKafkaTemplate.AGGREGATED_RESULTS_TOPIC);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	@Ignore("time sensitive")
	public void testAggregateTimeout() throws Exception {
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(E_REPLY, 0), 3);
		try {
			template.setReplyTimeout(5_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(E_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			try {
				future.get(30, TimeUnit.SECONDS);
				fail("Expected Exception");
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			}
			catch (ExecutionException e) {
				assertThat(e)
					.hasCauseExactlyInstanceOf(KafkaReplyTimeoutException.class)
					.hasMessageContaining("Reply timed out");
			}
			Thread.sleep(10_000);
			assertThat(KafkaTestUtils.getPropertyValue(template, "futures", Map.class)).isEmpty();
			assertThat(KafkaTestUtils.getPropertyValue(template, "pending", Map.class)).isEmpty();
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	@Ignore("time sensitive")
	public void testAggregateTimeoutPartial() throws Exception {
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(F_REPLY, 0), 3);
		template.setReturnPartialOnTimeout(true);
		try {
			template.setReplyTimeout(5_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(F_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, Collection<ConsumerRecord<Integer, String>>> consumerRecord =
					future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value().size()).isEqualTo(2);
			Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecord.value().iterator();
			String value1 = iterator.next().value();
			assertThat(value1).isIn("fOO", "FOO");
			String value2 = iterator.next().value();
			assertThat(value2).isIn("fOO", "FOO");
			assertThat(value2).isNotSameAs(value1);
			assertThat(consumerRecord.topic())
					.isEqualTo(AggregatingReplyingKafkaTemplate.PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testAggregateOrphansNotStored() throws Exception {
		GenericMessageListenerContainer container = mock(GenericMessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("two");
		properties.setAckMode(AckMode.MANUAL);
		given(container.getContainerProperties()).willReturn(properties);
		ProducerFactory pf = mock(ProducerFactory.class);
		Producer producer = mock(Producer.class);
		given(pf.createProducer(isNull())).willReturn(producer);
		AtomicReference<byte[]> correlation = new AtomicReference<>();
		willAnswer(invocation -> {
			ProducerRecord rec = invocation.getArgument(0);
			correlation.set(rec.headers().lastHeader(KafkaHeaders.CORRELATION_ID).value());
			return null;
		}).given(producer).send(any(), any());
		AggregatingReplyingKafkaTemplate template = new AggregatingReplyingKafkaTemplate(pf, container, coll -> true);
		template.setReplyTimeout(30_000);
		template.start();
		List<ConsumerRecord> records = new ArrayList<>();
		ConsumerRecord record = new ConsumerRecord("two", 0, 0L, null, "test1");
		RequestReplyFuture future = template.sendAndReceive(new ProducerRecord("one", null, "test"));
		record.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlation.get()));
		records.add(record);
		Consumer consumer = mock(Consumer.class);
		template.onMessage(records, consumer);
		assertThat(future.get(10, TimeUnit.SECONDS)).isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(template, "pending", Map.class)).hasSize(0);
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		offsets.put(new TopicPartition("two", 0), new OffsetAndMetadata(1));
		verify(consumer).commitSync(offsets, Duration.ofSeconds(30));
		// simulate redelivery after completion
		template.onMessage(records, consumer);
		assertThat(KafkaTestUtils.getPropertyValue(template, "pending", Map.class)).hasSize(0);
		template.stop();
		template.destroy();
	}

	public ReplyingKafkaTemplate<Integer, String, String> createTemplate(String topic) throws Exception {
		ContainerProperties containerProperties = new ContainerProperties(topic);
		final CountDownLatch latch = new CountDownLatch(1);
		containerProperties.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// no op
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				latch.countDown();
			}

		});
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.testName.getMethodName(), "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProperties);
		container.setBeanName(this.testName.getMethodName());
		ReplyingKafkaTemplate<Integer, String, String> template =
				new ReplyingKafkaTemplate<>(this.config.pf(), container);
		template.setSharedReplyTopic(true);
		template.start();
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(5);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic);
		return template;
	}

	public ReplyingKafkaTemplate<Integer, String, String> createTemplate(TopicPartitionOffset topic) {

		ContainerProperties containerProperties = new ContainerProperties(topic);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.testName.getMethodName(), "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProperties);
		container.setBeanName(this.testName.getMethodName());
		ReplyingKafkaTemplate<Integer, String, String> template = new ReplyingKafkaTemplate<>(this.config.pf(),
				container);
		template.setSharedReplyTopic(true);
		template.start();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(1);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic.getTopic());
		return template;
	}

	public AggregatingReplyingKafkaTemplate<Integer, String, String> aggregatingTemplate(
			TopicPartitionOffset topic, int releaseSize) {

		ContainerProperties containerProperties = new ContainerProperties(topic);
		containerProperties.setAckMode(AckMode.MANUAL_IMMEDIATE);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.testName.getMethodName(), "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, Collection<ConsumerRecord<Integer, String>>> cf =
				new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, Collection<ConsumerRecord<Integer, String>>> container =
				new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setBeanName(this.testName.getMethodName());
		AggregatingReplyingKafkaTemplate<Integer, String, String> template =
				new AggregatingReplyingKafkaTemplate<>(this.config.pf(), container, coll -> coll.size() == releaseSize);
		template.setSharedReplyTopic(true);
		template.start();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(1);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic.getTopic());
		return template;
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public DefaultKafkaProducerFactory<Integer, String> pf() {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> cf() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("serverSide", "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(pf());
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setReplyTemplate(template());
			factory.setReplyHeadersConfigurer((k, v) -> k.equals("baz"));
			factory.setMissingTopicsFatal(false);
			return factory;
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> simpleMapperFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setReplyTemplate(template());
			MessagingMessageConverter messageConverter = new MessagingMessageConverter();
			messageConverter.setHeaderMapper(new SimpleKafkaHeaderMapper());
			factory.setMessageConverter(messageConverter);
			factory.setReplyHeadersConfigurer(new ReplyHeadersConfigurer() {

				@Override
				public boolean shouldCopy(String headerName, Object headerValue) {
					return false;
				}

				@Override
				public Map<String, Object> additionalHeaders() {
					return Collections.singletonMap("qux", "fiz");
				}

			});
			return factory;
		}

		@KafkaListener(id = A_REQUEST, topics = A_REQUEST)
		@SendTo  // default REPLY_TOPIC header
		public String handleA(String in) {
			return in.toUpperCase();
		}

		@KafkaListener(topics = B_REQUEST, containerFactory = "simpleMapperFactory")
		@SendTo  // default REPLY_TOPIC header
		public String handleB(String in) {
			return in.substring(0, 1) + in.substring(1).toUpperCase();
		}

		@Bean
		public MultiMessageReturn mmr() {
			return new MultiMessageReturn();
		}

		@KafkaListener(id = "def1", topics = { D_REQUEST, E_REQUEST, F_REQUEST })
		@SendTo  // default REPLY_TOPIC header
		public String dListener1(String in) {
			return in.toUpperCase();
		}

		@KafkaListener(id = "def2", topics = { D_REQUEST, E_REQUEST, F_REQUEST })
		@SendTo  // default REPLY_TOPIC header
		public String dListener2(String in) {
			return in.substring(0, 1) + in.substring(1).toUpperCase();
		}

	}

	@KafkaListener(topics = C_REQUEST, groupId = C_REQUEST)
	@SendTo
	public static class MultiMessageReturn {

		@KafkaHandler
		public Message<?> listen1(String in, @Header(KafkaHeaders.REPLY_TOPIC) byte[] replyTo,
				@Header(KafkaHeaders.CORRELATION_ID) byte[] correlation) {
			return MessageBuilder.withPayload(in.toUpperCase())
					.setHeader(KafkaHeaders.TOPIC, replyTo)
					.setHeader(KafkaHeaders.MESSAGE_KEY, 42)
					.setHeader(KafkaHeaders.CORRELATION_ID, correlation)
					.build();
		}

	}

}
