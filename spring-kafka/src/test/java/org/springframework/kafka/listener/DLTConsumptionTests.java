/*
 * Copyright 2019 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.2.11
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class DLTConsumptionTests {

	private static final String TOPIC = "dltMain";

	private static final String DLT = "dltMain.DLT";

	@Autowired
	private Config config;

	@Test
	void testDLTConsumed(@Autowired KafkaTemplate<String, String> template) throws InterruptedException {
		template.send(TOPIC, "fail");
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch latch = new CountDownLatch(1);

		@Bean
		public EmbeddedKafkaBroker embeddedKafka() {
			return new EmbeddedKafkaBroker(1, true, 1, TOPIC, DLT);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			return factory(cf());
		}

		private ConcurrentKafkaListenerContainerFactory<String, String> factory(ConsumerFactory<String, String> cf) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(dltTemplate()), 1));
			return factory;
		}

		@Bean
		public ConsumerFactory<String, String> cf() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(TOPIC + ".g1", "false", embeddedKafka());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class.getName());
			props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, FailWhenPayloadIsFailDeserializer.class);
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ProducerFactory<String, String> pf() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafka()));
		}

		@Bean
		public KafkaTemplate<String, String> template() {
			return new KafkaTemplate<>(pf());
		}

		@Bean
		public ProducerFactory<Object, Object> dltPf() {
			Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean
		public KafkaTemplate<Object, Object> dltTemplate() {
			return new KafkaTemplate<>(dltPf());
		}

		@KafkaListener(id = TOPIC, topics = TOPIC)
		public void listen(@SuppressWarnings("unused") String in) {

		}

		@KafkaListener(id = DLT, topics = DLT,
				properties = "value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer")
		public void listen2(@SuppressWarnings("unused") ConsumerRecord<?, ?> in) {
			this.latch.countDown();
		}

	}

}
