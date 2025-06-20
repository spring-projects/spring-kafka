/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Soby Chacko
 * @since 3.2.7
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = { MicrometerMetricsTests.METRICS_TEST_TOPIC }, partitions = 1)
@DirtiesContext
public class MicrometerMetricsTests {

	public final static String METRICS_TEST_TOPIC = "metrics.test.topic";

	@Test
	void verifyMetricsWithoutObservation(@Autowired MetricsListener listener,
			@Autowired MeterRegistry meterRegistry,
			@Autowired KafkaTemplate<Integer, String> template)
			throws Exception {

		template.send(METRICS_TEST_TOPIC, "test").get(10, TimeUnit.SECONDS);
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		await().untilAsserted(() -> {
			Timer timer = meterRegistry.find("spring.kafka.listener")
					.tags("name", "metricsTest-0")
					.tag("result", "failure")
					.timer();

			assertThat(timer).isNotNull();
			assertThat(timer.getId().getTag("exception"))
					.isEqualTo("IllegalStateException");
		});
	}

	@Test
	void verifyMetricsWithObservation(@Autowired ObservationListener observationListener,
			@Autowired MeterRegistry meterRegistry,
			@Autowired KafkaTemplate<Integer, String> template)
			throws Exception {

		template.send(METRICS_TEST_TOPIC, "test").get(10, TimeUnit.SECONDS);
		assertThat(observationListener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		await().untilAsserted(() -> {
			Timer timer = meterRegistry.find("spring.kafka.listener")
					.tag("spring.kafka.listener.id", "observationTest-0")
					.tag("error", "IllegalStateException")
					.timer();

			assertThat(timer).isNotNull();
		});
	}

	@Configuration
	@EnableKafka
	static class Config {

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaProducerFactory<>(
					KafkaTestUtils.producerProps(broker));
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaConsumerFactory<>(
					KafkaTestUtils.consumerProps(broker, "metrics", false));
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setMicrometerEnabled(true);
			factory.getContainerProperties().setObservationEnabled(false);
			return factory;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> observationListenerContainerFactory(
				ConsumerFactory<Integer, String> cf, ObservationRegistry observationRegistry) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.getContainerProperties().setObservationRegistry(observationRegistry);
			return factory;
		}

		@Bean
		MetricsListener metricsListener() {
			return new MetricsListener();
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		ObservationListener observationListener() {
			return new ObservationListener();
		}

		@Bean
		ObservationRegistry observationRegistry(MeterRegistry meterRegistry) {
			ObservationRegistry observationRegistry = ObservationRegistry.create();
			observationRegistry.observationConfig()
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry));
			return observationRegistry;
		}
	}

	static class MetricsListener {
		final CountDownLatch latch = new CountDownLatch(1);

		@KafkaListener(id = "metricsTest", topics = METRICS_TEST_TOPIC)
		void listen(ConsumerRecord<Integer, String> in) {
			try {
				throw new IllegalStateException("metrics test exception");
			}
			finally {
				latch.countDown();
			}
		}
	}

	static class ObservationListener {
		final CountDownLatch latch = new CountDownLatch(1);

		@KafkaListener(id = "observationTest",
				topics = METRICS_TEST_TOPIC,
				containerFactory = "observationListenerContainerFactory")
		void listen(ConsumerRecord<Integer, String> in) {
			try {
				throw new IllegalStateException("observation test exception");
			}
			finally {
				latch.countDown();
			}
		}
	}

}

