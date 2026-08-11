/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression for GH-3947: mixed batch and non-batch listeners with observation enabled
 * must register {@code spring.kafka.listener} meters with a consistent tag key set.
 *
 * @author Hakaze Arimu
 * @since 4.2
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = {
		MixedBatchRecordObservationMetersTests.RECORD_TOPIC,
		MixedBatchRecordObservationMetersTests.BATCH_TOPIC,
		MixedBatchRecordObservationMetersTests.BATCH_FAIL_TOPIC
}, partitions = 1)
@DirtiesContext
public class MixedBatchRecordObservationMetersTests {

	public static final String RECORD_TOPIC = "mixed.obs.record";

	public static final String BATCH_TOPIC = "mixed.obs.batch";

	public static final String BATCH_FAIL_TOPIC = "mixed.obs.batch.fail";

	@Test
	void mixedBatchAndRecordListenersShareMeterTagKeys(
			@Autowired RecordListener recordListener,
			@Autowired BatchListener batchListener,
			@Autowired KafkaTemplate<Integer, String> template,
			@Autowired MeterRegistry meterRegistry)
			throws Exception {

		template.send(RECORD_TOPIC, "record-1").get(10, TimeUnit.SECONDS);
		template.send(BATCH_TOPIC, "batch-1").get(10, TimeUnit.SECONDS);

		assertThat(recordListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(batchListener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		await().untilAsserted(() -> {
			List<Timer> timers = meterRegistry.find("spring.kafka.listener").timers().stream().toList();
			assertThat(timers).hasSizeGreaterThanOrEqualTo(2);

			// Both listeners must contribute meters; legacy MicrometerHolder tags must not appear
			Set<String> allTagKeys = timers.stream()
					.flatMap(t -> t.getId().getTags().stream())
					.map(tag -> tag.getKey())
					.collect(Collectors.toSet());
			assertThat(allTagKeys)
					.contains("spring.kafka.listener.id", "messaging.system", "messaging.operation",
							"messaging.source.name", "messaging.source.kind", "messaging.kafka.consumer.group",
							"error")
					.doesNotContain("name", "result", "exception");

			// Every timer must use the same tag key set (Prometheus requirement)
			Set<Set<String>> tagKeySets = timers.stream()
					.map(t -> t.getId().getTags().stream().map(tag -> tag.getKey()).collect(Collectors.toSet()))
					.collect(Collectors.toSet());
			assertThat(tagKeySets)
					.as("all spring.kafka.listener timers must share one tag key set")
					.hasSize(1);

			// Both listener ids present
			Set<String> listenerIds = timers.stream()
					.map(t -> t.getId().getTag("spring.kafka.listener.id"))
					.collect(Collectors.toSet());
			assertThat(listenerIds).anyMatch(id -> id != null && id.startsWith("mixedRecord-"));
			assertThat(listenerIds).anyMatch(id -> id != null && id.startsWith("mixedBatch-"));
		});
	}

	@Test
	void batchListenerFailureRecordsObservationErrorNotLegacyExceptionTag(
			@Autowired FailingBatchListener failingBatchListener,
			@Autowired KafkaTemplate<Integer, String> template,
			@Autowired MeterRegistry meterRegistry)
			throws Exception {

		template.send(BATCH_FAIL_TOPIC, "batch-fail-1").get(10, TimeUnit.SECONDS);
		assertThat(failingBatchListener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		await().untilAsserted(() -> {
			// Observation-based meter records the failure under the "error" tag (not "none")
			List<Timer> failureTimers = meterRegistry.find("spring.kafka.listener")
					.tag("spring.kafka.listener.id", "mixedBatchFail-0")
					.timers()
					.stream()
					.filter(t -> {
						String error = t.getId().getTag("error");
						return error != null && !"none".equals(error);
					})
					.toList();
			assertThat(failureTimers)
					.as("batch failure should produce an observation meter with a non-none error tag")
					.isNotEmpty();
			assertThat(failureTimers.get(0).count()).isGreaterThanOrEqualTo(1);

			// Legacy MicrometerHolder failure timers use exception as a tag key — must not appear
			assertThat(meterRegistry.find("spring.kafka.listener").tagKeys("exception").timers()).isEmpty();
			assertThat(meterRegistry.find("spring.kafka.listener").tagKeys("name", "result", "exception").timers())
					.isEmpty();
		});
	}

	@Configuration
	@EnableKafka
	static class Config {

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker));
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaConsumerFactory<>(
					KafkaTestUtils.consumerProps(broker, "mixed-obs", false));
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		ObservationRegistry observationRegistry(MeterRegistry meterRegistry) {
			ObservationRegistry observationRegistry = ObservationRegistry.create();
			observationRegistry.observationConfig()
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry));
			return observationRegistry;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> recordFactory(
				ConsumerFactory<Integer, String> cf, ObservationRegistry observationRegistry) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setBatchListener(false);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.getContainerProperties().setObservationRegistry(observationRegistry);
			return factory;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> batchFactory(
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
		ConcurrentKafkaListenerContainerFactory<Integer, String> failingBatchFactory(
				ConsumerFactory<Integer, String> cf, ObservationRegistry observationRegistry) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setBatchListener(true);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.getContainerProperties().setObservationRegistry(observationRegistry);
			factory.getContainerProperties().setRecordObservationsInBatch(false);
			// Recover immediately so the test does not loop on the failed batch
			factory.setCommonErrorHandler(new DefaultErrorHandler((rec, ex) -> {
			}, new FixedBackOff(0L, 0L)));
			return factory;
		}

		@Bean
		RecordListener recordListener() {
			return new RecordListener();
		}

		@Bean
		BatchListener batchListener() {
			return new BatchListener();
		}

		@Bean
		FailingBatchListener failingBatchListener() {
			return new FailingBatchListener();
		}

	}

	static class RecordListener {

		final CountDownLatch latch = new CountDownLatch(1);

		@KafkaListener(id = "mixedRecord", topics = RECORD_TOPIC, containerFactory = "recordFactory")
		void listen(ConsumerRecord<Integer, String> in) {
			this.latch.countDown();
		}

	}

	static class BatchListener {

		final CountDownLatch latch = new CountDownLatch(1);

		@KafkaListener(id = "mixedBatch", topics = BATCH_TOPIC, containerFactory = "batchFactory")
		void listen(List<ConsumerRecord<Integer, String>> records) {
			this.latch.countDown();
		}

	}

	static class FailingBatchListener {

		final CountDownLatch latch = new CountDownLatch(1);

		@KafkaListener(id = "mixedBatchFail", topics = BATCH_FAIL_TOPIC, containerFactory = "failingBatchFactory")
		void listen(List<ConsumerRecord<Integer, String>> records) {
			this.latch.countDown();
			throw new RuntimeException("batch observation failure");
		}

	}

}
