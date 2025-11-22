/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.annotation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link KafkaListener} ackMode attribute.
 *
 * @author GO BEOMJUN
 * @since 4.1
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = {"ackModeRecord", "ackModeManual", "ackModeDefault"}, partitions = 1)
public class KafkaListenerAckModeTests {

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Test
	void testAckModeRecordOverride() throws Exception {
		this.template.send("ackModeRecord", "test-record");
		assertThat(this.config.recordLatch.await(10, TimeUnit.SECONDS)).isTrue();

		// Verify that the listener container has the correct ack mode
		MessageListenerContainer container = this.registry.getListenerContainer("ackModeRecordListener");
		assertThat(container).isNotNull();
		assertThat(container.getContainerProperties().getAckMode())
				.isEqualTo(ContainerProperties.AckMode.RECORD);
	}

	@Test
	void testAckModeManualOverride() throws Exception {
		this.template.send("ackModeManual", "test-manual");
		assertThat(this.config.manualLatch.await(10, TimeUnit.SECONDS)).isTrue();

		// Verify that the listener container has the correct ack mode
		MessageListenerContainer container = this.registry.getListenerContainer("ackModeManualListener");
		assertThat(container).isNotNull();
		assertThat(container.getContainerProperties().getAckMode())
				.isEqualTo(ContainerProperties.AckMode.MANUAL);
	}

	@Test
	void testAckModeDefault() throws Exception {
		this.template.send("ackModeDefault", "test-default");
		assertThat(this.config.defaultLatch.await(10, TimeUnit.SECONDS)).isTrue();

		// Verify that the listener container uses factory default (BATCH)
		MessageListenerContainer container = this.registry.getListenerContainer("ackModeDefaultListener");
		assertThat(container).isNotNull();
		assertThat(container.getContainerProperties().getAckMode())
				.isEqualTo(ContainerProperties.AckMode.BATCH);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch recordLatch = new CountDownLatch(1);

		final CountDownLatch manualLatch = new CountDownLatch(1);

		final CountDownLatch defaultLatch = new CountDownLatch(1);

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps(broker, "testGroup", false));
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker));
		}

		@Bean
		public KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> consumerFactory) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			// Set factory default to BATCH
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
			return factory;
		}

		@KafkaListener(id = "ackModeRecordListener", topics = "ackModeRecord", ackMode = "RECORD")
		public void listenWithRecordAck(String message) {
			this.recordLatch.countDown();
		}

		@KafkaListener(id = "ackModeManualListener", topics = "ackModeManual", ackMode = "MANUAL")
		public void listenWithManualAck(String message, Acknowledgment ack) {
			ack.acknowledge();
			this.manualLatch.countDown();
		}

		@KafkaListener(id = "ackModeDefaultListener", topics = "ackModeDefault")
		public void listenWithDefaultAck(String message) {
			this.defaultLatch.countDown();
		}

	}

}
