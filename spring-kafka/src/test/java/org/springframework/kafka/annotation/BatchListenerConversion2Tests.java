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
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Gary Russell
 *
 * @since 2.1.1
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class BatchListenerConversion2Tests {

	private static final String DEFAULT_TEST_GROUP_ID = "blc2";

	@ClassRule // one topic to preserve order
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 1, "blc.2.1");

	@Autowired
	private Config config;

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Test
	public void testBatchOfPojosWithABadOne() throws Exception {
		Listener listener = this.config.listener1();
		String topic = "blc.2.1";
		this.template.send(topic, "{\"bar\":\"baz\"}");
		this.template.send(topic, "junk");
		this.template.send(topic, "{\"bar\":\"baz\"}");
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.badFoo).isInstanceOf(BadFoo.class);
		assertThat(listener.receivedFoos).isEqualTo(2);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, Foo> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setReplyTemplate(template());
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, Foo> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", embeddedKafka.getEmbeddedKafka());
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
			consumerProps.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
			consumerProps.put(ErrorHandlingDeserializer2.VALUE_FUNCTION, FailedFooProvider.class);
			consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Foo.class.getName());
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			return kafkaTemplate;
		}

		@Bean
		public BytesJsonMessageConverter converter() {
			return new BytesJsonMessageConverter();
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return props;
		}

		@Bean
		public Listener listener1() {
			return new Listener();
		}

	}

	public static class Listener {

		private final CountDownLatch latch1 = new CountDownLatch(3);

		private volatile Foo badFoo;

		private volatile int receivedFoos;

		@KafkaListener(id = "deser", topics = "blc.2.1")
		public void listen1(List<Foo> foos) {
			foos.forEach(f -> {
				if (f.getBar() == null) {
					this.badFoo = f;
				}
				else {
					this.receivedFoos++;
				}
				this.latch1.countDown();
			});
		}

	}

	public static class Foo {

		private String bar;

		public Foo() {
			super();
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

	public static class BadFoo extends Foo {

		private final byte[] failedDecode;

		public BadFoo(byte[] failedDecode) {
			this.failedDecode = failedDecode;
		}

		public byte[] getFailedDecode() {
			return this.failedDecode;
		}

	}

	public static class FailedFooProvider implements BiFunction<byte[], Headers, Foo> {

		@Override
		public Foo apply(byte[] t, Headers u) {
			return new BadFoo(t);
		}

	}

}
