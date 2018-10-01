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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class AliasPropertiesTests {

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Autowired
	private Config config;

	@Test
	public void testAliasFor() throws Exception {
		this.template.send("alias.tests", "foo");
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch = new CountDownLatch(1);

		@Bean
		public EmbeddedKafkaBroker embeddedKafka() {
			return new EmbeddedKafkaBroker(1, true, "alias.tests");
		}

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps("myAliasGroup", "false", embeddedKafka());
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
			return KafkaTestUtils.producerProps(embeddedKafka());
		}

		@MyListener("alias.tests")
		public void listen1(String in) {
			latch.countDown();
		}

	}

	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@KafkaListener
	public @interface MyListener {

		@AliasFor(annotation = KafkaListener.class, attribute = "topics")
		String[] value();

	}

}
