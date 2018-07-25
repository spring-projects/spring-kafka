/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.3
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class StatefulRetryTests {

	private static final String DEFAULT_TEST_GROUP_ID = "statefulRetry";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 1, "sr1");

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

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setErrorHandler(new SeekToCurrentErrorHandler() {

				@Override
				public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
						Consumer<?, ?> consumer, MessageListenerContainer container) {
					Config.this.seekPerformed = true;
					super.handle(thrownException, records, consumer, container);
				}

			});
			factory.setStatefulRetry(true);
			factory.setRetryTemplate(new RetryTemplate());
			factory.setRecoveryCallback(c -> {
				this.latch2.countDown();
				return null;
			});
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", embeddedKafka.getEmbeddedKafka());
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(producerFactory());
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());
		}

		@KafkaListener(id = "retry", topics = "sr1", groupId = "sr1")
		public void listen1(String in) {
			this.latch1.countDown();
			throw new RuntimeException("retry");
		}

	}

}
