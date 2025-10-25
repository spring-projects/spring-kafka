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
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
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
 * Reproduces and verifies the fix for the issue described in GH-4097.
 *
 * @author George Mahfoud
 * @since 3.3.11
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "smartBatchTopic" })
class BatchSmartMessageConverterTests {

	@Autowired
	private KafkaTemplate<Integer, byte[]> template;

	@Autowired
	private Config config;

	@Test
	void testContentTypeConverterWithBatchListener() throws Exception {
		// Given: A batch listener with contentTypeConverter configured
		BatchListener listener = this.config.batchListener();

		// When: Send byte[] messages that should be converted to String
		this.template.send("smartBatchTopic", "hello".getBytes());
		this.template.send("smartBatchTopic", "world".getBytes());

		// Then: SmartMessageConverter should convert byte[] to String for batch listener
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.received).hasSize(2).containsExactly("hello", "world");
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
			// Set up batch converter with record converter - framework will propagate SmartMessageConverter
			factory.setBatchMessageConverter(new BatchMessagingMessageConverter(new MessagingMessageConverter()));
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
			return new ByteArrayToStringConverter();
		}

		@Bean
		public BatchListener batchListener() {
			return new BatchListener();
		}

	}

	public static class BatchListener {

		private final CountDownLatch latch = new CountDownLatch(2);

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

	}

	/**
	 * Simple SmartMessageConverter for testing that converts byte[] to String.
	 */
	static class ByteArrayToStringConverter implements SmartMessageConverter {

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			Object payload = message.getPayload();
			return (payload instanceof byte[] bytes) ? new String(bytes) : payload;
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
