/*
 * Copyright 2026-present the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Minchul Son
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = AsyncMonoDefaultErrorHandlerRetryTests.TOPIC, partitions = 1)
class AsyncMonoDefaultErrorHandlerRetryTests {

	static final String TOPIC = "async-mono-default-eh-retry";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private Listener listener;

	@Test
	void retriesMonoErrorWithDefaultErrorHandler() throws Exception {
		this.kafkaTemplate.send(TOPIC, "data").get(10, TimeUnit.SECONDS);

		assertThat(this.listener.invocations.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.retryAttempts.await(30, TimeUnit.SECONDS)).isTrue();
	}

	static class Listener {

		final CountDownLatch invocations = new CountDownLatch(3);

		final CountDownLatch retryAttempts = new CountDownLatch(3);

		@KafkaListener(id = "asyncMonoDefaultEhRetry", topics = TOPIC)
		Mono<Void> listen(String value) {
			this.invocations.countDown();
			throw new IllegalStateException("mono failure: " + value);
		}

	}

	@Configuration
	@EnableKafka
	static class Config {

		@Bean
		Listener listener() {
			return new Listener();
		}

		@Bean
		KafkaTemplate<String, String> kafkaTemplate(EmbeddedKafkaBroker embeddedKafka) {
			return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafka)));
		}

		@Bean
		DefaultKafkaConsumerFactory<String, String> consumerFactory(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka,
					"async-mono-default-eh-group", false);
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		CommonErrorHandler commonErrorHandler(Listener listener) {
			DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1L, 2L));
			errorHandler.setRetryListeners((consumerRecord, ex, deliveryAttempt) -> listener.retryAttempts.countDown());
			return errorHandler;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				DefaultKafkaConsumerFactory<String, String> consumerFactory, CommonErrorHandler commonErrorHandler) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setCommonErrorHandler(commonErrorHandler);
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
			factory.getContainerProperties().setAsyncAcks(true);
			return factory;
		}

	}

}
