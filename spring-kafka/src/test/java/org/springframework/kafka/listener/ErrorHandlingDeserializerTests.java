/*
 * Copyright 2018-2019 the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
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
		assertThat(this.config.goodCount.get()).withFailMessage("Counts wrong: %s", this.config).isEqualTo(2);
		assertThat(this.config.keyErrorCount.get()).withFailMessage("Counts wrong: %s", this.config).isEqualTo(2);
		assertThat(this.config.valueErrorCount.get()).withFailMessage("Counts wrong: %s", this.config).isEqualTo(2);
		assertThat(this.config.headers).isNotNull();
	}

	@Test
	public void unitTests() throws Exception {
		ErrorHandlingDeserializer2<String> ehd = new ErrorHandlingDeserializer2<>(new StringDeserializer());
		assertThat(ehd.deserialize("topic", "foo".getBytes())).isEqualTo("foo");
		ehd.close();
		ehd = new ErrorHandlingDeserializer2<>(new Deserializer<String>() {

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
			}

			@Override
			public String deserialize(String topic, byte[] data) {
				throw new RuntimeException("fail");
			}

			@Override
			public void close() {
			}

		});
		Headers headers = new RecordHeaders();
		Object result = ehd.deserialize("topic", headers, "foo".getBytes());
		assertThat(result).isNull();
		Header deser = headers.lastHeader(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_EXCEPTION_HEADER);
		assertThat(new ObjectInputStream(new ByteArrayInputStream(deser.value())).readObject())
				.isInstanceOf(DeserializationException.class);
		ehd.close();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch = new CountDownLatch(6);

		private final AtomicInteger goodCount = new AtomicInteger();

		private final AtomicInteger keyErrorCount = new AtomicInteger();

		private final AtomicInteger valueErrorCount = new AtomicInteger();

		private Headers headers;

		@KafkaListener(topics = TOPIC)
		public void listen1(ConsumerRecord<String, String> record) {
			this.goodCount.incrementAndGet();
			this.latch.countDown();
		}

		@KafkaListener(topics = TOPIC, containerFactory = "kafkaListenerContainerFactoryExplicitDesers")
		public void listen2(ConsumerRecord<String, String> record) {
			this.goodCount.incrementAndGet();
			this.latch.countDown();
		}

		@Bean
		public EmbeddedKafkaBroker embeddedKafka() {
			return new EmbeddedKafkaBroker(1, true, 1, TOPIC);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			return factory(cf());
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryExplicitDesers() {
			return factory(cfWithExplicitDeserializers());
		}

		private ConcurrentKafkaListenerContainerFactory<String, String> factory(ConsumerFactory<String, String> cf) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setErrorHandler((t, r) -> {
				if (r.value() == null && t instanceof DeserializationException) {
					this.valueErrorCount.incrementAndGet();
					this.headers = ((DeserializationException) t).getHeaders();
				}
				else if (r.key() == null && t instanceof DeserializationException) {
					this.keyErrorCount.incrementAndGet();
				}
				this.latch.countDown();
			});
			return factory;
		}

		@Bean
		public ConsumerFactory<String, String> cf() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(TOPIC + ".g1", "false", embeddedKafka());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
			props.put(ErrorHandlingDeserializer2.KEY_DESERIALIZER_CLASS, FailWhenPayloadIsFailDeserializer.class);
			props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS,
					FailWhenPayloadIsFailDeserializer.class.getName());
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ConsumerFactory<String, String> cfWithExplicitDeserializers() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(TOPIC + ".g2", "false", embeddedKafka());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return new DefaultKafkaConsumerFactory<>(props,
					new ErrorHandlingDeserializer2<String>(new FailWhenPayloadIsFailDeserializer())
							.keyDeserializer(true),
					new ErrorHandlingDeserializer2<String>(new FailWhenPayloadIsFailDeserializer()));
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

		@Override
		public String toString() {
			return "Config [goodCount=" + this.goodCount.get() + ", keyErrorCount=" + this.keyErrorCount.get()
					+ ", valueErrorCount=" + this.valueErrorCount.get() + "]";
		}

	}

}
