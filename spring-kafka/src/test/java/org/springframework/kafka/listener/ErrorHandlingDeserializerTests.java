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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
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
public class ErrorHandlingDeserializerTests {

	private static final String TOPIC = "ehdt";

	@Autowired
	public Config config;

	@Test
	public void testBadDeserializer() throws Exception {
		this.config.template().send(TOPIC, "foo", "bar");
		this.config.template().send(TOPIC, "fail", "bar");
		this.config.template().send(TOPIC, "foo", "fail");
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.goodCount).isEqualTo(1);
		assertThat(this.config.keyErrorCount).isEqualTo(1);
		assertThat(this.config.valueErrorCount).isEqualTo(1);
		assertThat(this.config.headers).isNotNull();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch = new CountDownLatch(3);

		private int goodCount;

		private int keyErrorCount;

		private int valueErrorCount;

		private Headers headers;

		@KafkaListener(topics = TOPIC)
		public void listen(ConsumerRecord<String, String> record) {
			this.goodCount++;
			this.latch.countDown();
		}


		@Bean
		public EmbeddedKafkaBroker embeddedKafka() {
			return new EmbeddedKafkaBroker(1, true, 1, TOPIC);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setErrorHandler((t, r) -> {
				if (r.value() instanceof DeserializationException) {
					this.valueErrorCount++;
					this.headers = ((DeserializationException) r.value()).getHeaders();
				}
				else if (r.key() instanceof DeserializationException) {
					this.keyErrorCount++;
				}
				this.latch.countDown();
			});
			return factory;
		}

		@Bean
		public ConsumerFactory<String, String> cf() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(TOPIC, "false", embeddedKafka());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
			props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, FailSometimesDeserializer.class);
			props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, FailSometimesDeserializer.class.getName());
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ProducerFactory<String, String> pf() {
			Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka());
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean
		public KafkaTemplate<String, String> template() {
			return new KafkaTemplate<>(pf());
		}

	}

	public static class FailSometimesDeserializer implements ExtendedDeserializer<String> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public String deserialize(String topic, byte[] data) {
			return new String(data);
		}

		@Override
		public void close() {
		}

		@Override
		public String deserialize(String topic, Headers headers, byte[] data) {
			String string = new String(data);
			if ("fail".equals(string)) {
				throw new RuntimeException("fail");
			}
			return string;
		}

	}

}
