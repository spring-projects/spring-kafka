/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Verify that autoStartDltHandler overrides factory autoStartup (for both factory
 * settings).
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
@SpringJUnitConfig
@EmbeddedKafka
public class DltStartupTests {

	@Test
	void dltStartOverridesCorrect(@Autowired KafkaListenerEndpointRegistry registry) {
		// using RetryTopicConfiguration
		// factory with autostartup = true
		assertThat(registry.getListenerContainer("shouldStartDlq1").isRunning()).isTrue();
		assertThat(registry.getListenerContainer("shouldStartDlq1-dlt").isRunning()).isTrue();
		assertThat(registry.getListenerContainer("shouldNotStartDlq2").isRunning()).isTrue();
		assertThat(registry.getListenerContainer("shouldNotStartDlq2-dlt").isRunning()).isFalse();
		// factory with autostartup = false
		assertThat(registry.getListenerContainer("shouldNotStartDlq3").isRunning()).isFalse();
		assertThat(registry.getListenerContainer("shouldNotStartDlq3-dlt").isRunning()).isFalse();
		assertThat(registry.getListenerContainer("shouldStartDlq4").isRunning()).isFalse();
		assertThat(registry.getListenerContainer("shouldStartDlq4-dlt").isRunning()).isTrue();

		// using @RetryableTopic
		// factory with autostartup = true
		assertThat(registry.getListenerContainer("shouldStartDlq5").isRunning()).isTrue();
		assertThat(registry.getListenerContainer("shouldStartDlq5-dlt").isRunning()).isTrue();
		assertThat(registry.getListenerContainer("shouldNotStartDlq6").isRunning()).isTrue();
		assertThat(registry.getListenerContainer("shouldNotStartDlq6-dlt").isRunning()).isFalse();
		// factory with autostartup = false
		assertThat(registry.getListenerContainer("shouldNotStartDlq7").isRunning()).isFalse();
		assertThat(registry.getListenerContainer("shouldNotStartDlq7-dlt").isRunning()).isFalse();
		assertThat(registry.getListenerContainer("shouldStartDlq8").isRunning()).isFalse();
		assertThat(registry.getListenerContainer("shouldStartDlq8-dlt").isRunning()).isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config extends RetryTopicConfigurationSupport {

		@KafkaListener(id = "shouldStartDlq1", topics = "DltStartupTests.1", containerFactory = "cf1")
		void shouldStartDlq1(String in) {
		}

		@KafkaListener(id = "shouldNotStartDlq2", topics = "DltStartupTests.2", containerFactory = "cf1")
		void shouldNotStartDlq2(String in) {
		}

		@KafkaListener(id = "shouldNotStartDlq3", topics = "DltStartupTests.3", containerFactory = "cf2")
		void shouldNotStartDlq3(String in) {
		}

		@KafkaListener(id = "shouldStartDlq4", topics = "DltStartupTests.4", containerFactory = "cf2")
		void shoulStartDlq4(String in) {
		}

		@KafkaListener(id = "shouldStartDlq5", topics = "DltStartupTests.5", containerFactory = "cf1")
		@RetryableTopic(attempts = "1", kafkaTemplate = "template")
		void shouldStartDlq5(String in) {
		}

		@KafkaListener(id = "shouldNotStartDlq6", topics = "DltStartupTests.6", containerFactory = "cf1")
		@RetryableTopic(attempts = "1", kafkaTemplate = "template", autoStartDltHandler = "false")
		void shouldNotStartDlq6(String in) {
		}

		@KafkaListener(id = "shouldNotStartDlq7", topics = "DltStartupTests.7", containerFactory = "cf2")
		@RetryableTopic(attempts = "1", kafkaTemplate = "template")
		void shouldNotStartDlq7(String in) {
		}

		@KafkaListener(id = "shouldStartDlq8", topics = "DltStartupTests.8", containerFactory = "cf2")
		@RetryableTopic(attempts = "1", kafkaTemplate = "template", autoStartDltHandler = "true")
		void shoulStartDlq8(String in) {
		}

		@Bean
		RetryTopicConfiguration rtc1(KafkaOperations<Integer, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.maxAttempts(1)
					.includeTopic("DltStartupTests.1")
					.create(template);
		}

		@Bean
		RetryTopicConfiguration rtc2(KafkaOperations<Integer, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.maxAttempts(1)
					.includeTopic("DltStartupTests.2")
					.autoStartDltHandler(false) // override factory for DLT container
					.create(template);
		}

		@Bean
		RetryTopicConfiguration rtc3(KafkaOperations<Integer, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.maxAttempts(1)
					.includeTopic("DltStartupTests.3")
					.create(template);
		}

		@Bean
		RetryTopicConfiguration rtc4(KafkaOperations<Integer, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.maxAttempts(1)
					.includeTopic("DltStartupTests.4")
					.autoStartDltHandler(true) // override factory for DLT container
					.create(template);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> cf1(ConsumerFactory<Integer, String> cf) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			return factory;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> cf2(ConsumerFactory<Integer, String> cf) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setAutoStartup(false);
			return factory;
		}

		@Bean
		ConsumerFactory<Integer, String> cf(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("dltStart", "false", broker));
		}

		@Bean
		KafkaAdmin admin(EmbeddedKafkaBroker broker) {
			return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
		}

		@Bean
		@SuppressWarnings("unchecked")
		KafkaOperations<Integer, String> template() {
			return mock(KafkaOperations.class);
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

	}

}
