/*
 * Copyright 2018 the original author or authors.
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
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

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 5, A_REQUEST, A_REPLY,
			B_REQUEST, B_REPLY);

	@Rule
	public TestName testName = new TestName();

	@Autowired
	private Config config;

	@Test
	public void testGood() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(A_REQUEST, "foo");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, A_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
		}
		finally {
			template.stop();
		}
	}

	@Test
	public void testGoodSamePartition() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(A_REQUEST, 2, null, "foo");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, A_REPLY.getBytes()));
			record.headers()
					.add(new RecordHeader(KafkaHeaders.REPLY_PARTITION, new byte[] { 0, 0, 0, 2 }));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
			assertThat(consumerRecord.partition()).isEqualTo(2);
		}
		finally {
			template.stop();
		}
	}

	@Test
	public void testTimeout() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setReplyTimeout(1);
			ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(A_REQUEST, "foo");
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
				assertThat(e).hasCauseExactlyInstanceOf(KafkaException.class).hasMessageContaining("Reply timed out");
			}
		}
		finally {
			template.stop();
		}
	}

	@Test
	public void testGoodWithSimpleMapper() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(B_REPLY);
		try {
			template.setReplyTimeout(30_000);
			ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(B_REQUEST, "qux");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, B_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("qUX");
		}
		finally {
			template.stop();
		}
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
		ReplyingKafkaTemplate<Integer, String, String> template = new ReplyingKafkaTemplate<>(this.config.pf(), container);
		template.start();
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(5);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic);
		return template;
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public DefaultKafkaProducerFactory<Integer, String> pf() {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
			return pf;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> cf() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("serverSide", "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
			return cf;
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
			return factory;
		}

		@KafkaListener(topics = A_REQUEST)
		@SendTo  // default REPLY_TOPIC header
		public String handleA(String in) {
			return in.toUpperCase();
		}

		@KafkaListener(topics = B_REQUEST, containerFactory = "simpleMapperFactory")
		@SendTo  // default REPLY_TOPIC header
		public String handleB(String in) {
			return in.substring(0, 1) + in.substring(1).toUpperCase();
		}

	}

}
