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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;
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

/**
 * Tests for batch individual record observation functionality.
 *
 * @author Igor Quintanilha
 * @author Artem Bilan
 *
 * @since 4.0
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
			@Autowired KafkaTemplate<Integer, String> template, @Autowired TestObservationHandler observationHandler,
			@Autowired SimpleTracer tracer)
			throws InterruptedException {

		// Clear any existing observations and spans
		observationHandler.clear();
		tracer.getSpans().clear();

		// Send multiple messages
		template.send(BATCH_INDIVIDUAL_OBSERVATION_TOPIC, "message-1");
		template.send(BATCH_INDIVIDUAL_OBSERVATION_TOPIC, "message-2");
		template.send(BATCH_INDIVIDUAL_OBSERVATION_TOPIC, "message-3");

		// Wait for batch processing
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		// With batch individual record observation enabled, we should get observations for individual records
		assertThat(observationHandler.getStartedObservations())
				.as("Should create observations when batch individual record observation is enabled")
				.isEqualTo(3);

		// Verify that producer and consumer observations are created
		var spans = new ArrayList<>(tracer.getSpans());
		var producerSpans = spans.stream()
				.filter(span -> "PRODUCER".equals(span.getKind().name()))
				.toList();
		var consumerSpans = spans.stream()
				.filter(span -> "CONSUMER".equals(span.getKind().name()))
				.toList();

		assertThat(producerSpans)
				.as("Should have 3 producer spans")
				.hasSize(3);

		assertThat(consumerSpans)
				.as("Should have 3 consumer spans for individual records")
				.hasSize(3);

		// Verify propagation worked - each consumer span should have the propagated values
		// And verify that consumer spans are in the correct order
		assertThat(consumerSpans)
				.as("Should have exactly 3 consumer spans")
				.hasSize(3);

		// Verify first consumer span has msg-1
		assertThat(consumerSpans.get(0).getTags())
				.as("First consumer span should have propagated values from first producer")
				.containsEntry("foo", "some foo value")
				.containsEntry("bar", "some bar value")
				.containsEntry("message-id", "msg-1");

		// Verify second consumer span has msg-2
		assertThat(consumerSpans.get(1).getTags())
				.as("Second consumer span should have propagated values from second producer")
				.containsEntry("foo", "some foo value")
				.containsEntry("bar", "some bar value")
				.containsEntry("message-id", "msg-2");

		// Verify third consumer span has msg-3
		assertThat(consumerSpans.get(2).getTags())
				.as("Third consumer span should have propagated values from third producer")
				.containsEntry("foo", "some foo value")
				.containsEntry("bar", "some bar value")
				.containsEntry("message-id", "msg-3");
	}

	@Test
	void batchIndividualRecordObservationDisabledCreatesNoIndividualObservations(
			@Autowired BatchListenerWithoutIndividualObservation batchListener,
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
			Map<String, Object> configs = KafkaTestUtils.consumerProps(broker, "batch-tests", false);
			configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 100);
			configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 2000);
			return new DefaultKafkaConsumerFactory<>(configs);
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf,
				ObservationRegistry observationRegistry) {

			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			template.setObservationRegistry(observationRegistry);
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
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		SimpleTracer simpleTracer() {
			return new SimpleTracer();
		}

		@Bean
		Propagator propagator(Tracer tracer) {
			return new Propagator() {

				private final AtomicInteger messageCounter = new AtomicInteger(0);

				// List of headers required for tracing propagation
				@Override
				public List<String> fields() {
					return Arrays.asList("foo", "bar", "message-id");
				}

				// This is called on the producer side when the message is being sent
				@Override
				public <C> void inject(TraceContext context, @Nullable C carrier, Setter<C> setter) {
					setter.set(carrier, "foo", "some foo value");
					setter.set(carrier, "bar", "some bar value");
					// Add unique message identifier
					String messageId = "msg-" + messageCounter.incrementAndGet();
					setter.set(carrier, "message-id", messageId);
				}

				// This is called on the consumer side when the message is consumed
				@Override
				public <C> Span.Builder extract(C carrier, Getter<C> getter) {
					String foo = getter.get(carrier, "foo");
					String bar = getter.get(carrier, "bar");
					String messageId = getter.get(carrier, "message-id");
					return tracer.spanBuilder()
							.tag("foo", foo)
							.tag("bar", bar)
							.tag("message-id", messageId);
				}
			};
		}

		@Bean
		ObservationRegistry observationRegistry(Tracer tracer, Propagator propagator, MeterRegistry meterRegistry,
				TestObservationHandler testObservationHandler) {

			ObservationRegistry observationRegistry = ObservationRegistry.create();
			observationRegistry.observationConfig()
					.observationHandler(
							// Composite will pick the first matching handler
							new ObservationHandler.FirstMatchingCompositeObservationHandler(
									// This is responsible for creating a child span on the sender side
									new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
									// This is responsible for creating a span on the receiver side
									new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
									// This is responsible for creating a default span
									new DefaultTracingObservationHandler(tracer)))
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry))
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

		CountDownLatch latch = new CountDownLatch(3);

		@KafkaListener(topics = BATCH_INDIVIDUAL_OBSERVATION_TOPIC,
				containerFactory = "observationListenerContainerFactory")
		public void listen(List<ConsumerRecord<Integer, String>> records) {
			for (ConsumerRecord<Integer, String> record : records) {
				latch.countDown();
			}
		}

	}

	static class BatchListenerWithoutIndividualObservation {

		CountDownLatch latch = new CountDownLatch(2);

		@KafkaListener(topics = BATCH_ONLY_OBSERVATION_TOPIC,
				containerFactory = "batchOnlyObservationListenerContainerFactory")

		public void listen(List<ConsumerRecord<Integer, String>> records) {
			for (ConsumerRecord<Integer, String> record : records) {
				latch.countDown();
			}
		}

	}

	static class TestObservationHandler implements ObservationHandler<Observation.Context> {

		private final AtomicInteger startedObservations = new AtomicInteger(0);

		@Override
		public void onStart(Observation.Context context) {
			if (!(context instanceof KafkaRecordReceiverContext)) {
				return; // Ignore if not a valid observation context
			}
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
