/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Manoel Angelo
 *
 * @since 4.3
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = RetryTopicMainTopicDelayIntegrationTests.MAIN_TOPIC, partitions = 1)
class RetryTopicMainTopicDelayIntegrationTests {

	static final String MAIN_TOPIC = "RetryTopicMainTopicDelayIntegrationTests.main";

	static final long MAIN_TOPIC_DELAY = 2000;

	@Test
	void shouldDelayTheFirstProcessingAttempt(@Autowired KafkaTemplate<String, String> template,
			@Autowired DelayedListener listener) throws InterruptedException {

		template.send(MAIN_TOPIC, "foo");

		assertThat(listener.latch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.receivedAt - listener.recordTimestamp).isGreaterThanOrEqualTo(MAIN_TOPIC_DELAY);
	}

	static class DelayedListener {

		private final CountDownLatch latch = new CountDownLatch(1);

		private volatile long recordTimestamp;

		private volatile long receivedAt;

		@RetryableTopic(mainTopicDelay = "" + MAIN_TOPIC_DELAY)
		@KafkaListener(id = "mainTopicDelay", topics = MAIN_TOPIC)
		void listen(ConsumerRecord<String, String> record) {
			this.receivedAt = System.currentTimeMillis();
			this.recordTimestamp = record.timestamp();
			this.latch.countDown();
		}

	}

	@Configuration(proxyBeanMethods = false)
	@EnableKafka
	static class Config {

		@Bean
		DelayedListener delayedListener() {
			return new DelayedListener();
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.getContainerProperties().setPollTimeout(50L);
			factory.getContainerProperties().setIdlePartitionEventInterval(100L);
			return factory;
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "mainTopicDelay", false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker));
		}

		@Bean
		KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}

		@Bean
		KafkaAdmin admin(EmbeddedKafkaBroker broker) {
			return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

	}

}
