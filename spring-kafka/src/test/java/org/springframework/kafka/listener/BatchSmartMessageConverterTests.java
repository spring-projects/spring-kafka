/*
 * Copyright 2016-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SmartMessageConverter support in batch listeners.
 *
 * @author George Mahfoud
 * @since 3.3.11
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "smartBatchTopic", "smartBatchTopic2" })
class BatchSmartMessageConverterTests {

	@Autowired
	private KafkaTemplate<Integer, byte[]> template;

	@Autowired
	private Config config;

	@Test
	void testContentTypeConverterWithBatchListener() throws Exception {
		BatchListener listener = this.config.batchListener();
		listener.reset(2);

		this.template.send("smartBatchTopic", "hello".getBytes());
		this.template.send("smartBatchTopic", "world".getBytes());

		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.received).containsExactly("hello", "world");
	}

	@Test
	void testMultipleListenersWithDifferentConverters() throws Exception {
		BatchListener listener1 = this.config.batchListener();
		BatchListener2 listener2 = this.config.batchListener2();
		listener1.reset(1);
		listener2.reset(1);

		String listener1Data = "listener1Data";
		String listener2Data = "listener2Data";
		this.template.send("smartBatchTopic", listener1Data.getBytes());
		this.template.send("smartBatchTopic2", listener2Data.getBytes());

		assertThat(listener1.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener2.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener1.received).containsExactly(listener1Data);
		assertThat(listener2.received).containsExactly(listener2Data.toUpperCase());
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(EmbeddedKafkaBroker embeddedKafka) {
			ConcurrentKafkaListenerContainerFactory<Integer, byte[]> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory(embeddedKafka));
			factory.setBatchListener(true);
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, byte[]> consumerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> consumerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(embeddedKafka, "smartBatchGroup", false);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, byte[]> template(EmbeddedKafkaBroker embeddedKafka) {
			return new KafkaTemplate<>(producerFactory(embeddedKafka));
		}

		@Bean
		public ProducerFactory<Integer, byte[]> producerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaProducerFactory<>(producerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> producerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
			return props;
		}

		@Bean
		public SmartMessageConverter byteArrayToStringConverter() {
			return new ByteArrayConverter(bytes -> new String(bytes));
		}

		@Bean
		public SmartMessageConverter byteArrayToUpperCaseConverter() {
			return new ByteArrayConverter(bytes -> new String(bytes).toUpperCase());
		}

		@Bean
		public BatchListener batchListener() {
			return new BatchListener();
		}

		@Bean
		public BatchListener2 batchListener2() {
			return new BatchListener2();
		}

	}

	public static class BatchListener {

		private CountDownLatch latch = new CountDownLatch(2);

		private final List<String> received = new ArrayList<>();

		@KafkaListener(
				id = "batchSmartListener",
				topics = "smartBatchTopic",
				groupId = "smartBatchGroup",
				contentTypeConverter = "byteArrayToStringConverter",
				batch = "true"
		)
		public void listen(List<String> messages) {
			messages.forEach(message -> {
				this.received.add(message);
				this.latch.countDown();
			});
		}

		void reset(int expectedCount) {
			this.received.clear();
			this.latch = new CountDownLatch(expectedCount);
		}

	}

	public static class BatchListener2 {

		private CountDownLatch latch = new CountDownLatch(1);

		private final List<String> received = new ArrayList<>();

		@KafkaListener(
				id = "batchSmartListener2",
				topics = "smartBatchTopic2",
				groupId = "smartBatchGroup2",
				contentTypeConverter = "byteArrayToUpperCaseConverter",
				batch = "true"
		)
		public void listen(List<String> messages) {
			messages.forEach(message -> {
				this.received.add(message);
				this.latch.countDown();
			});
		}

		void reset(int expectedCount) {
			this.received.clear();
			this.latch = new CountDownLatch(expectedCount);
		}

	}

	/**
	 * Simple SmartMessageConverter for testing that converts byte[] to String using a function.
	 */
	static class ByteArrayConverter implements SmartMessageConverter {

		private final java.util.function.Function<byte[], String> converter;

		ByteArrayConverter(java.util.function.Function<byte[], String> converter) {
			this.converter = converter;
		}

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			Object payload = message.getPayload();
			return (payload instanceof byte[] bytes) ? this.converter.apply(bytes) : payload;
		}

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
			return fromMessage(message, targetClass);
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers) {
			return MessageBuilder.withPayload(payload).copyHeaders(headers).build();
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint) {
			return toMessage(payload, headers);
		}

	}
}
