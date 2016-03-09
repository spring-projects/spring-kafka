/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.SimpleKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Gary Russell
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class EnableKafkaIntegrationTests {

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "annotated1", "annotated2", "annotated3",
			"annotated4", "annotated5", "annotated6");

	@Autowired
	public Listener listener;

	@Autowired
	public KafkaTemplate<Integer, String> template;

	@Autowired
	public KafkaListenerEndpointRegistry registry;

	@Test
	public void testSimple() throws Exception {
		waitListening("foo");
		template.convertAndSend("annotated1", 0, "foo");
		template.flush();
		assertTrue(this.listener.latch1.await(10, TimeUnit.SECONDS));

		waitListening("bar");
		template.convertAndSend("annotated2", 0, "foo");
		template.flush();
		assertTrue(this.listener.latch2.await(10, TimeUnit.SECONDS));
		assertNotNull(this.listener.partition);

		waitListening("baz");
		template.convertAndSend("annotated3", 0, "foo");
		template.flush();
		assertTrue(this.listener.latch3.await(10, TimeUnit.SECONDS));
		assertEquals("foo", this.listener.record.value());

		waitListening("qux");
		template.convertAndSend("annotated4", 0, "foo");
		template.flush();
		assertTrue(this.listener.latch4.await(10, TimeUnit.SECONDS));
		assertEquals("foo", this.listener.record.value());
		assertNotNull(this.listener.ack);

		waitListening("fiz");
		template.convertAndSend("annotated5", 0, 0, "foo");
		template.convertAndSend("annotated5", 1, 0, "bar");
		template.convertAndSend("annotated6", 0, 0, "baz");
		template.convertAndSend("annotated6", 1, 0, "qux");
		template.flush();
		assertTrue(this.listener.latch5.await(10, TimeUnit.SECONDS));
	}

	private void waitListening(String id) throws InterruptedException {
		MessageListenerContainer container = registry.getListenerContainer(id);
		@SuppressWarnings("unchecked")
		KafkaMessageListenerContainer<Integer, String> kmlc =
				((ConcurrentMessageListenerContainer<Integer, String>) container).getContainers().get(0);
		int n = 0;
		while (n++ < 6000 && (kmlc.getAssignedPartitions() == null || kmlc.getAssignedPartitions().size() == 0)) {
			Thread.sleep(100);
		}
		assertTrue(kmlc.getAssignedPartitions().size() > 0);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
		kafkaListenerContainerFactory() {
			SimpleKafkaListenerContainerFactory<Integer, String> factory = new SimpleKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
		kafkaManualAckListenerContainerFactory() {
			SimpleKafkaListenerContainerFactory<Integer, String> factory = new SimpleKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(manualConsumerFactory());
			factory.setAckMode(AckMode.MANUAL_IMMEDIATE);
			return factory;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public ConsumerFactory<Integer, String> manualConsumerFactory() {
			Map<String, Object> configs = consumerConfigs();
			configs.put("enable.auto.commit", "false");
			return new DefaultKafkaConsumerFactory<>(configs);
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
//			props.put("bootstrap.servers", "localhost:9092");
			props.put("group.id", "testAnnot");
			props.put("enable.auto.commit", true);
			props.put("auto.commit.interval.ms", "100");
			props.put("session.timeout.ms", "15000");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			return props;
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
//			props.put("bootstrap.servers", "localhost:9092");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			return props;
		}

		@Bean
		public KafkaTemplate<Integer, String> kafkaTemplate() {
			return new KafkaTemplate<Integer, String>(producerFactory());
		}

	}

	public static class Listener {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private final CountDownLatch latch3 = new CountDownLatch(1);

		private final CountDownLatch latch4 = new CountDownLatch(1);

		private final CountDownLatch latch5 = new CountDownLatch(1);

		private volatile Integer partition;

		private volatile ConsumerRecord<?, ?> record;

		private volatile Acknowledgment ack;

		@KafkaListener(id = "foo", topics = "annotated1")
		public void listen1(String foo) {
			this.latch1.countDown();
		}

		@KafkaListener(id = "bar", topicPattern = "annotated2")
		public void listen2(@Payload String foo, @Header(KafkaHeaders.PARTITION_ID) int partitionHeader) {
			this.partition = partitionHeader;
			this.latch2.countDown();
		}

		@KafkaListener(id = "baz", topicPartitions = @TopicPartition(topic = "annotated3", partitions = "0"))
		public void listen3(ConsumerRecord<?, ?> record) {
			this.record = record;
			this.latch3.countDown();
		}

		@KafkaListener(id = "qux", topics = "annotated4", containerFactory = "kafkaManualAckListenerContainerFactory")
		public void listen4(@Payload String foo, Acknowledgment ack) {
			this.ack = ack;
			this.ack.acknowledge();
			this.latch4.countDown();
		}

		@KafkaListener(id = "fiz", topicPartitions =
				{ @TopicPartition(topic = "annotated5", partitions = { "0", "1" }),
				  @TopicPartition(topic = "annotated6", partitions = { "0", "1" })
				})
		public void listen5(ConsumerRecord<?, ?> record) {
			this.record = record;
			this.latch5.countDown();
		}

	}

}
