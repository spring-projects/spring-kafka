/*
 * Copyright 2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.7.3
 *
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = "ContainerGroupSequencerTests")
public class ContainerGroupSequencerTests {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ContainerGroupSequencerTests.class));

	@Test
	void sequenceCompletes(@Autowired Config config, @Autowired KafkaTemplate<Integer, String> template)
			throws InterruptedException {

		template.send("ContainerGroupSequencerTests", "test");
		assertThat(config.stopped.await(10, TimeUnit.SECONDS))
				.as("stopped latch still has a count of %d", config.stopped.getCount())
				.isTrue();
		List<String> order = config.order;
		assertThat(order.get(0))
				.as("out of order %s")
				.isIn("one", "two");
		assertThat(order.get(1))
				.as("out of order %s")
				.isIn("one", "two");
		assertThat(order.get(2))
				.as("out of order %s")
				.isIn("three", "four");
		assertThat(order.get(3))
				.as("out of order %s")
				.isIn("three", "four");
		assertThat(config.receivedAt.get(3) - config.receivedAt.get(0)).isGreaterThanOrEqualTo(1000);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch stopped = new CountDownLatch(12); // 8 children, 4 parents

		final List<String> order = Collections.synchronizedList(new ArrayList<>());

		final List<Long> receivedAt = Collections.synchronizedList(new ArrayList<>());


		@KafkaListener(id = "one", topics = "ContainerGroupSequencerTests", containerGroup = "g1", concurrency = "2")
		public void listen1(String in) {
			this.order.add("one");
			this.receivedAt.add(System.currentTimeMillis());
		}

		@KafkaListener(id = "two", topics = "ContainerGroupSequencerTests", containerGroup = "g1", concurrency = "2")
		public void listen2(String in) {
			this.order.add("two");
			this.receivedAt.add(System.currentTimeMillis());
		}

		@KafkaListener(id = "three", topics = "ContainerGroupSequencerTests", containerGroup = "g2", concurrency = "2")
		public void listen3(String in) {
			this.order.add("three");
			this.receivedAt.add(System.currentTimeMillis());
		}

		@KafkaListener(id = "four", topics = "ContainerGroupSequencerTests", containerGroup = "g2", concurrency = "2")
		public void listen4(String in) {
			this.order.add("four");
			this.receivedAt.add(System.currentTimeMillis());
		}

		@EventListener
		public void stopped(ContainerStoppedEvent event) {
			LOGGER.debug(() -> event.toString());
			this.stopped.countDown();
		}

		@Bean
		ContainerGroupSequencer sequencer(KafkaListenerEndpointRegistry registry) {
			ContainerGroupSequencer sequencer = new ContainerGroupSequencer(registry, 1000, "g1", "g2");
			sequencer.setStopLastGroupWhenIdle(true);
			return sequencer;
		}

		@Bean
		ProducerFactory<Integer, String> pf(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker));
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		ConsumerFactory<Integer, String> cf(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("", "false", broker));
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setPollTimeout(500);
			return factory;
		}

	}

}
