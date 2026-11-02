/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import javax.net.ServerSocketFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class AddressableEmbeddedBrokerTests {

	private static final String TEST_EMBEDDED = "testEmbedded";

	@Autowired
	private Config config;

	@Autowired
	private EmbeddedKafkaKraftBroker broker;

	@Test
	public void testKafkaEmbedded() {
		//TODO: Static port assignments in KRAFT mode in KafkaClusterTestKit
//		assertThat(broker.getBrokersAsString()).isEqualTo("localhost:" + this.config.kafkaPort);
		assertThat(broker.getBrokersAsString())
				.isEqualTo(System.getProperty(EmbeddedKafkaKraftBroker.SPRING_EMBEDDED_KAFKA_BROKERS));
	}

	@Test
	public void testLateStartedConsumer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.broker, TEST_EMBEDDED, false);
		Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		this.broker.consumeFromAnEmbeddedTopic(consumer, TEST_EMBEDDED);

		Producer<String, Object> producer = new KafkaProducer<>(KafkaTestUtils.producerProps(this.broker));
		producer.send(new ProducerRecord<>(TEST_EMBEDDED, "foo"));
		producer.close();
		KafkaTestUtils.getSingleRecord(consumer, TEST_EMBEDDED);

		consumerProps = KafkaTestUtils.consumerProps(this.broker, "another" + TEST_EMBEDDED, false);
		Consumer<Integer, String> consumer2 = new KafkaConsumer<>(consumerProps);
		this.broker.consumeFromAnEmbeddedTopic(consumer2, TEST_EMBEDDED);
		KafkaTestUtils.getSingleRecord(consumer2, TEST_EMBEDDED);

		consumer.close();
		consumer2.close();
	}

	@Configuration
	public static class Config {

		private int kafkaPort;

		@Bean
		public EmbeddedKafkaKraftBroker broker() throws IOException {
			ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0);
			this.kafkaPort = ss.getLocalPort();
			ss.close();
			EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, "topic1", TEST_EMBEDDED);
			kafka.kafkaPorts(this.kafkaPort);
			kafka.afterPropertiesSet();


			return kafka;
		}

	}

}
