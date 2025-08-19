/*
 * Copyright 2022-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaMessageHeaderAccessor;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test delivery header, since 3.2 support class level {@link RetryableTopic}.
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @since 2.8.11
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { "dh1", DeliveryHeaderTests.DH_CLASS_LEVEL_1 })
public class DeliveryHeaderTests {

	static final String DH_CLASS_LEVEL_1 = "dhClassLevel1";

	@Test
	void deliveryAttempts(@Autowired Config config, @Autowired KafkaTemplate<Integer, String> template,
			@Autowired RetryTopicClassLevel retryTopicClassLevel)
			throws InterruptedException {

		template.send("dh1", "test");
		template.send(DH_CLASS_LEVEL_1, "test");
		assertThat(config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(retryTopicClassLevel.latchClassLevel.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(config.attempts.toString())
				.isEqualTo("[[1, 1], [2, 1], [3, 1], [1, 2], [2, 2], [3, 2], [1, 3], [2, 3], [3, 3]]");
		assertThat(config.accessorAttempts.toString())
				.isEqualTo("[[1, 1], [2, 1], [3, 1], [1, 2], [2, 2], [3, 2], [1, 3], [2, 3], [3, 3]]");

		assertThat(retryTopicClassLevel.attemptsClassLevel.toString())
				.isEqualTo("[[1, 1], [2, 1], [3, 1], [1, 2], [2, 2], [3, 2], [1, 3], [2, 3], [3, 3]]");
		assertThat(retryTopicClassLevel.accessorAttemptsClassLevel.toString())
				.isEqualTo("[[1, 1], [2, 1], [3, 1], [1, 2], [2, 2], [3, 2], [1, 3], [2, 3], [3, 3]]");
	}

	@Configuration
	@EnableKafka
	public static class Config extends RetryTopicConfigurationSupport {

		@Autowired
		EmbeddedKafkaBroker broker;

		List<List<Integer>> attempts = new ArrayList<>();

		List<List<Integer>> accessorAttempts = new ArrayList<>();

		CountDownLatch latch = new CountDownLatch(9);

		@Override
		protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
			blockingRetries.retryOn(RuntimeException.class)
					.backOff(new FixedBackOff(0, 2));
		}

		@Override
		protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
			return DeadLetterPublishingRecovererFactory::neverLogListenerException;
		}

		@RetryableTopic(backOff = @BackOff(maxDelay = 0))
		@KafkaListener(id = "dh1", topics = "dh1")
		void listen(String in, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int blockingAttempts,
				@Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) Integer nonBlockingAttempts,
				KafkaMessageHeaderAccessor accessor) {

			this.attempts.add(List.of(blockingAttempts, nonBlockingAttempts == null ? 1 : nonBlockingAttempts));
			this.accessorAttempts.add(List.of(accessor.getBlockingRetryDeliveryAttempt(),
					accessor.getNonBlockingRetryDeliveryAttempt()));
			this.latch.countDown();
			throw new RuntimeException("test");
		}

		@Bean
		RetryTopicClassLevel retryTopicClassLevel() {
			return new RetryTopicClassLevel();
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		ProducerFactory<Integer, String> pf() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(this.broker));
		}

		@Bean
		@Primary
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setDeliveryAttemptHeader(true);
			return factory;
		}

		@Bean
		@Primary
		ConsumerFactory<Integer, String> cf() {
			return new DefaultKafkaConsumerFactory<>(
					KafkaTestUtils.consumerProps(this.broker, "dh1", false));
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

	}

	@RetryableTopic(backOff = @BackOff(maxDelay = 0))
	@KafkaListener(id = "dhClassLevel1", topics = DH_CLASS_LEVEL_1)
	static class RetryTopicClassLevel {

		List<List<Integer>> attemptsClassLevel = new ArrayList<>();

		List<List<Integer>> accessorAttemptsClassLevel = new ArrayList<>();

		CountDownLatch latchClassLevel = new CountDownLatch(9);

		@KafkaHandler
		void listen(String in, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int blockingAttempts,
					@Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false) Integer nonBlockingAttempts,
					KafkaMessageHeaderAccessor accessor) {

			this.attemptsClassLevel.add(List.of(blockingAttempts, nonBlockingAttempts == null ? 1 : nonBlockingAttempts));
			this.accessorAttemptsClassLevel.add(List.of(accessor.getBlockingRetryDeliveryAttempt(),
					accessor.getNonBlockingRetryDeliveryAttempt()));
			this.latchClassLevel.countDown();
			throw new RuntimeException("test");
		}

	}

}
