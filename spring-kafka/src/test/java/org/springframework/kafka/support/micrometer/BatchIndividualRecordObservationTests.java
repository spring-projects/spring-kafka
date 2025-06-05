/*
 * Copyright 2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
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

/**
 * Tests for batch individual record observation functionality.
 *
 * @author Igor Quintanilha
 * @since 3.4
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = {BatchIndividualRecordObservationTests.BATCH_INDIVIDUAL_OBSERVATION_TOPIC, 
		BatchIndividualRecordObservationTests.BATCH_ONLY_OBSERVATION_TOPIC}, partitions = 1)
@DirtiesContext
public class BatchIndividualRecordObservationTests {

	public static final String BATCH_INDIVIDUAL_OBSERVATION_TOPIC = "batch.individual.observation";

	public static final String BATCH_ONLY_OBSERVATION_TOPIC = "batch.only.observation";

	@Test
	void batchIndividualRecordObservationCreatesObservationPerRecord(@Autowired BatchListener listener,
			@Autowired KafkaTemplate<Integer, String> template, @Autowired TestObservationHandler observationHandler)
			throws InterruptedException {

		// Clear any existing observations
		observationHandler.clear();

		// Send multiple messages
		template.send(BATCH_INDIVIDUAL_OBSERVATION_TOPIC, "message-1");
		template.send(BATCH_INDIVIDUAL_OBSERVATION_TOPIC, "message-2");
		template.send(BATCH_INDIVIDUAL_OBSERVATION_TOPIC, "message-3");

		// Wait for batch processing
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		// With batch individual record observation enabled, we should get observations for individual records
		assertThat(observationHandler.getStartedObservations())
				.as("Should create observations when batch individual record observation is enabled")
				.isPositive();

		assertThat(listener.processedRecords).hasSize(3);
	}

	@Test
	void batchIndividualRecordObservationDisabledCreatesNoIndividualObservations(@Autowired BatchListenerWithoutIndividualObservation batchListener,
			@Autowired KafkaTemplate<Integer, String> template, @Autowired TestObservationHandler observationHandler)
			throws InterruptedException {

		// Clear any existing observations
		observationHandler.clear();

		// Send messages
		template.send(BATCH_ONLY_OBSERVATION_TOPIC, "batch-message-1");
		template.send(BATCH_ONLY_OBSERVATION_TOPIC, "batch-message-2");

		// Wait for batch processing
		assertThat(batchListener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		// When individual record observation is disabled, no individual observations should be created
		assertThat(observationHandler.getStartedObservations())
				.as("No individual observations should be created when batch individual observation is disabled")
				.isZero();

		assertThat(batchListener.processedRecords).hasSize(2);
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
					KafkaTestUtils.consumerProps("batch-tests", "false", broker));
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setBatchListener(true);
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
			factory.setBatchListener(true);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.getContainerProperties().setObservationRegistry(observationRegistry);
			factory.getContainerProperties().setRecordObservationsInBatch(true);
			return factory;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> batchOnlyObservationListenerContainerFactory(
				ConsumerFactory<Integer, String> cf, ObservationRegistry observationRegistry) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setBatchListener(true);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.getContainerProperties().setObservationRegistry(observationRegistry);
			factory.getContainerProperties().setRecordObservationsInBatch(false);
			return factory;
		}

		@Bean
		TestObservationHandler testObservationHandler() {
			return new TestObservationHandler();
		}

		@Bean
		ObservationRegistry observationRegistry(TestObservationHandler testObservationHandler) {
			ObservationRegistry observationRegistry = ObservationRegistry.create();
			observationRegistry.observationConfig()
					.observationHandler(testObservationHandler);
			return observationRegistry;
		}

		@Bean
		BatchListener batchListener() {
			return new BatchListener();
		}

		@Bean
		BatchListenerWithoutIndividualObservation batchListenerWithoutIndividualObservation() {
			return new BatchListenerWithoutIndividualObservation();
		}
	}

	static class BatchListener {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<String> processedRecords = new ArrayList<>();

		@KafkaListener(topics = BATCH_INDIVIDUAL_OBSERVATION_TOPIC, containerFactory = "observationListenerContainerFactory")
		public void listen(List<ConsumerRecord<Integer, String>> records) {
			for (ConsumerRecord<Integer, String> record : records) {
				processedRecords.add(record.value());
			}
			latch.countDown();
		}
	}

	static class BatchListenerWithoutIndividualObservation {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<String> processedRecords = new ArrayList<>();

		@KafkaListener(topics = BATCH_ONLY_OBSERVATION_TOPIC, containerFactory = "batchOnlyObservationListenerContainerFactory")
		public void listen(List<ConsumerRecord<Integer, String>> records) {
			for (ConsumerRecord<Integer, String> record : records) {
				processedRecords.add(record.value());
			}
			latch.countDown();
		}
	}

	static class TestObservationHandler implements ObservationHandler<Observation.Context> {
		
		private final AtomicInteger startedObservations = new AtomicInteger(0);

		@Override
		public void onStart(Observation.Context context) {
			startedObservations.incrementAndGet();
		}

		@Override
		public void onStop(Observation.Context context) {
			// No-op for this test
		}

		@Override
		public void onError(Observation.Context context) {
			// No-op for this test
		}

		@Override
		public boolean supportsContext(Observation.Context context) {
			return true;
		}

		public int getStartedObservations() {
			return startedObservations.get();
		}

		public void clear() {
			startedObservations.set(0);
		}
	}

}
