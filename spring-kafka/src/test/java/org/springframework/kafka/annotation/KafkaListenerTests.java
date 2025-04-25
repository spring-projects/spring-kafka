/*
 * Copyright 2016-2025 the original author or authors.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerTests.Config.ClassAndMethodLevelListener;
import org.springframework.kafka.annotation.KafkaListenerTests.Config.ClassLevelListenerWithDoublePublicMethod;
import org.springframework.kafka.annotation.KafkaListenerTests.Config.ClassLevelListenerWithSinglePublicMethod;
import org.springframework.kafka.annotation.KafkaListenerTests.Config.ClassLevelListenerWithSinglePublicMethodAndPrivateMethod;
import org.springframework.kafka.annotation.KafkaListenerTests.Config.OtherClassLevelListenerWithSinglePublicMethod;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Sanghyeok An
 *
 * @since 4.0
 *
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = "default-listener.tests")
public class KafkaListenerTests {

	private static final String TEST_TOPIC = "default-listener.tests";

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@Autowired
	private ClassAndMethodLevelListener classLevel;

	@Autowired
	private ClassLevelListenerWithSinglePublicMethod classLevelListenerWithSinglePublicMethod;

	@Autowired
	private OtherClassLevelListenerWithSinglePublicMethod otherClassLevelListenerWithSinglePublicMethod;

	@Autowired
	private ClassLevelListenerWithDoublePublicMethod classLevelListenerWithDoublePublicMethod;

	@Autowired
	private ClassLevelListenerWithSinglePublicMethodAndPrivateMethod classLevelListenerWithSinglePublicMethodAndPrivateMethod;

	@Test
	public void testImplicitKafkaHandlerAnnotation() throws Exception {
		// GIVEN
		// See, bean configuration.

		// WHEN
		this.template.send(TEST_TOPIC, "foo");

		// THEN
		assertThat(this.classLevel.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.classLevelListenerWithSinglePublicMethod.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.otherClassLevelListenerWithSinglePublicMethod.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.classLevelListenerWithSinglePublicMethodAndPrivateMethod.latch.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(this.classLevelListenerWithDoublePublicMethod.latch.await(10, TimeUnit.SECONDS)).isFalse();
		assertThat(this.classLevelListenerWithDoublePublicMethod.latch.getCount()).isEqualTo(ClassLevelListenerWithDoublePublicMethod.INIT_LATCH_COUNT);

		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("classAndMethodLevelListener").getGroupId())
				.isEqualTo("classAndMethodLevelListener");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("classLevelListenerWithSinglePublicMethod").getGroupId())
				.isEqualTo("classLevelListenerWithSinglePublicMethod");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("otherClassLevelListenerWithSinglePublicMethod").getGroupId())
				.isEqualTo("otherClassLevelListenerWithSinglePublicMethod");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("classLevelListenerWithDoublePublicMethod").getGroupId())
				.isEqualTo("classLevelListenerWithDoublePublicMethod");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("classLevelListenerWithSinglePublicMethodAndPrivateMethod").getGroupId())
				.isEqualTo("classLevelListenerWithSinglePublicMethodAndPrivateMethod");

	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

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
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
					this.broker.getBrokersAsString(),
					"myDefaultListenerGroup",
					"false");
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
			return KafkaTestUtils.producerProps(this.broker.getBrokersAsString());
		}

		@Component
		@KafkaListener(id = "classAndMethodLevelListener", topics = "default-listener.tests")
		public static class ClassAndMethodLevelListener {

			final CountDownLatch latch = new CountDownLatch(1);

			@KafkaHandler
			public void listen(String in) {
				this.latch.countDown();
			}
		}

		@Component
		@KafkaListener(id = "classLevelListenerWithSinglePublicMethod", topics = TEST_TOPIC)
		public static class ClassLevelListenerWithSinglePublicMethod {

			final CountDownLatch latch = new CountDownLatch(1);

			public void listen(String in) {
				this.latch.countDown();
			}
		}

		@Component
		@KafkaListener(id = "otherClassLevelListenerWithSinglePublicMethod", topics = TEST_TOPIC)
		public static class OtherClassLevelListenerWithSinglePublicMethod {

			final CountDownLatch latch = new CountDownLatch(1);

			public void listen(String in) {
				this.latch.countDown();
			}
		}

		@Component
		@KafkaListener(id = "classLevelListenerWithDoublePublicMethod", topics = TEST_TOPIC)
		public static class ClassLevelListenerWithDoublePublicMethod {

			public final static int INIT_LATCH_COUNT = 2;

			final CountDownLatch latch = new CountDownLatch(INIT_LATCH_COUNT);

			public void listen1(String in) {
				this.latch.countDown();
			}

			public void listen2(String in) {
				this.latch.countDown();
			}
		}

		@Component
		@KafkaListener(id = "classLevelListenerWithSinglePublicMethodAndPrivateMethod", topics = TEST_TOPIC)
		public static class ClassLevelListenerWithSinglePublicMethodAndPrivateMethod {

			final CountDownLatch latch = new CountDownLatch(1);

			public void listen1(String in) {
				this.latch.countDown();
			}

			private void listen2(String in) {
				this.latch.countDown();
			}

			private void listen3(String in) {
				this.latch.countDown();
			}

		}

	}

}
