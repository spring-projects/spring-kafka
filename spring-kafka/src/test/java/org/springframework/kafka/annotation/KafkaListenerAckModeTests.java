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

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link KafkaListener} ackMode attribute.
 *
 * @author Go BeomJun
 *
 * @since 4.1
 */
@SpringJUnitConfig
public class KafkaListenerAckModeTests {

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Test
	void testAckModeRecordOverride() {
		MessageListenerContainer container = this.registry.getListenerContainer("ackModeRecordListener");
		assertThat(container).isNotNull();
		assertThat(container.getContainerProperties().getAckMode())
				.isEqualTo(ContainerProperties.AckMode.RECORD);
	}

	@Test
	void testAckModeManualOverride() {
		MessageListenerContainer container = this.registry.getListenerContainer("ackModeManualListener");
		assertThat(container).isNotNull();
		assertThat(container.getContainerProperties().getAckMode())
				.isEqualTo(ContainerProperties.AckMode.MANUAL);
	}

	@Test
	void testAckModeDefault() {
		MessageListenerContainer container = this.registry.getListenerContainer("ackModeDefaultListener");
		assertThat(container).isNotNull();
		assertThat(container.getContainerProperties().getAckMode())
				.isEqualTo(ContainerProperties.AckMode.BATCH);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
			return mock();
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
			factory.setConsumerFactory(consumerFactory);
			// Set factory default to BATCH
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
			return factory;
		}

		@KafkaListener(id = "ackModeRecordListener", topics = "ackModeRecord", ackMode = "RECORD", autoStartup = "false")
		public void listenWithRecordAck(String message) {
		}

		@KafkaListener(id = "ackModeManualListener", topics = "ackModeManual", ackMode = "MANUAL", autoStartup = "false")
		public void listenWithManualAck(String message, Acknowledgment ack) {
		}

		@KafkaListener(id = "ackModeDefaultListener", topics = "ackModeDefault", autoStartup = "false")
		public void listenWithDefaultAck(String message) {
		}

	}

}
