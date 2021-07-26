/*
 * Copyright 2018-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException.Level;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.3
 *
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = "sr1", partitions = 1)
public class StatefulRetryTests {

	private static final String DEFAULT_TEST_GROUP_ID = "statefulRetry";

	@Autowired
	private Config config;

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Test
	public void testStatefulRetry() throws Exception {
		this.template.send("sr1", "foo");
		assertThat(this.config.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.seekPerformed).isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch1 = new CountDownLatch(3);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private boolean seekPerformed;

		@SuppressWarnings("deprecation")
		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(EmbeddedKafkaBroker embeddedKafka) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory(embeddedKafka));
			SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler() {

				@Override
				public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
						Consumer<?, ?> consumer, MessageListenerContainer container) {
					Config.this.seekPerformed = true;
					super.handle(thrownException, records, consumer, container);
				}

			};
			errorHandler.setLogLevel(Level.INFO);
			factory.setErrorHandler(errorHandler);
			factory.setStatefulRetry(true);
			factory.setRetryTemplate(new RetryTemplate());
			factory.setRecoveryCallback(c -> {
				this.latch2.countDown();
				return null;
			});
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> consumerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", embeddedKafka);
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, String> template(EmbeddedKafkaBroker embeddedKafka) {
			return new KafkaTemplate<>(producerFactory(embeddedKafka));
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaProducerFactory<>(producerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> producerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			return KafkaTestUtils.producerProps(embeddedKafka);
		}

		@KafkaListener(id = "retry", topics = "sr1", groupId = "sr1")
		public void listen1(String in) {
			this.latch1.countDown();
			throw new RuntimeException("retry");
		}

	}

}
