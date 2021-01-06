/*
 * Copyright 2017-2021 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "blc1", "blc2", "blc3", "blc4", "blc5" })
public class BatchListenerConversionTests {

	private static final String DEFAULT_TEST_GROUP_ID = "blc";

	@Autowired
	private Config config;

	@Autowired
	private Listener listener1;

	@Autowired
	private Listener listener2;

	@Autowired
	private KafkaTemplate<Integer, Foo> template;

	@Test
	public void testBatchOfPojos() throws Exception {
		doTest(this.listener1, "blc1");
		doTest(this.listener2, "blc2");
	}

	private void doTest(Listener listener, String topic) throws InterruptedException {
		this.template.send(new GenericMessage<>(
				new Foo("bar"), Collections.singletonMap(KafkaHeaders.TOPIC, topic)));
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.received.size()).isGreaterThan(0);
		assertThat(listener.received.get(0)).isInstanceOf(Foo.class);
		assertThat(listener.received.get(0).bar).isEqualTo("bar");
		assertThat((listener.receivedTopics).get(0)).isEqualTo(topic);
		assertThat((listener.receivedPartitions).get(0)).isEqualTo(0);
	}

	@Test
	public void testBatchOfPojoMessages() throws Exception {
		String topic = "blc3";
		this.template.send(new GenericMessage<>(
				new Foo("bar"), Collections.singletonMap(KafkaHeaders.TOPIC, topic)));
		Listener3 listener = this.config.listener3();
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.received.size()).isGreaterThan(0);
		assertThat(listener.received.get(0).getPayload()).isInstanceOf(Foo.class);
		assertThat(listener.received.get(0).getPayload().getBar()).isEqualTo("bar");
	}

	@Test
	public void testBatchReplies() throws Exception {
		Listener4 listener = this.config.listener4();
		String topic = "blc4";
		this.template.send(new GenericMessage<>(
				new Foo("bar"), Collections.singletonMap(KafkaHeaders.TOPIC, topic)));
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.received.size()).isGreaterThan(0);
		assertThat(listener.received.get(0)).isInstanceOf(Foo.class);
		assertThat(listener.received.get(0).bar).isEqualTo("bar");
		assertThat(listener.replies.size()).isGreaterThan(0);
		assertThat(listener.replies.get(0)).isInstanceOf(Foo.class);
		assertThat(listener.replies.get(0).bar).isEqualTo("BAR");
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(EmbeddedKafkaBroker embeddedKafka) {
			ConcurrentKafkaListenerContainerFactory<Integer, Foo> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory(embeddedKafka));
			factory.setBatchListener(true);
			factory.setMessageConverter(new BatchMessagingMessageConverter(converter()));
			factory.setReplyTemplate(template(embeddedKafka));
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, Foo> consumerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> consumerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, Foo> template(EmbeddedKafkaBroker embeddedKafka) {
			KafkaTemplate<Integer, Foo> kafkaTemplate = new KafkaTemplate<>(producerFactory(embeddedKafka));
			kafkaTemplate.setMessageConverter(converter());
			return kafkaTemplate;
		}

		@Bean
		public BytesJsonMessageConverter converter() {
			return new BytesJsonMessageConverter();
		}

		@Bean
		public ProducerFactory<Integer, Foo> producerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaProducerFactory<>(producerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> producerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class);
			return props;
		}

		@Bean
		public Listener listener1(KafkaListenerContainerFactory<?> cf) {
			return new Listener("blc1", cf);
		}

		@Bean
		public Listener listener2(KafkaListenerContainerFactory<?> cf) {
			return new Listener("blc2", cf);
		}

		@Bean
		public Listener3 listener3() {
			return new Listener3();
		}

		@Bean
		public Listener4 listener4() {
			return new Listener4();
		}

	}

	public static class Listener {

		private final String topic;

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private final KafkaListenerContainerFactory<?> cf;

		private List<Foo> received;

		private List<String> receivedTopics;

		private List<Integer> receivedPartitions;

		public Listener(String topic, KafkaListenerContainerFactory<?> cf) {
			this.topic = topic;
			this.cf = cf;
		}

		public KafkaListenerContainerFactory<?> getContainerFactory() {
			return this.cf;
		}

		@KafkaListener(topics = "#{__listener.topic}", groupId = "#{__listener.topic}.group",
				containerFactory = "#{__listener.containerFactory}")
		// @SendTo("foo") test WARN log for void return
		public void listen1(List<Foo> foos, @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions) {
			if (this.received == null) {
				this.received = foos;
			}
			this.receivedTopics = topics;
			this.receivedPartitions = partitions;
			this.latch1.countDown();
		}

		@KafkaListener(beanRef = "__x", topics = "#{__x.topic}", groupId = "#{__x.topic}.group2")
		public void listen2(List<Foo> foos) {
			this.latch2.countDown();
		}

		public String getTopic() {
			return this.topic;
		}

	}

	public static class Listener3 {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private List<Message<Foo>> received;

		@KafkaListener(topics = "blc3", groupId = "blc3")
		public void listen1(List<Message<Foo>> foos) {
			if (this.received == null) {
				this.received = foos;
			}
			this.latch1.countDown();
		}

	}

	public static class Listener4 {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private List<Foo> received;

		private List<Foo> replies;

		@KafkaListener(topics = "blc4", groupId = "blc4")
		@SendTo
		public Collection<Message<?>> listen1(List<Foo> foos) {
			if (this.received == null) {
				this.received = foos;
			}
			return foos.stream().map(f -> MessageBuilder.withPayload(new Foo(f.getBar().toUpperCase()))
					.setHeader(KafkaHeaders.TOPIC, "blc5")
					.setHeader(KafkaHeaders.MESSAGE_KEY, 42)
					.build())
					.collect(Collectors.toList());
		}

		@KafkaListener(topics = "blc5", groupId = "blc5")
		public void listen2(List<Foo> foos) {
			this.replies = foos;
			this.latch1.countDown();
		}

	}

	public static class Foo {

		public String bar;

		public Foo() {
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

}
