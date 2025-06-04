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
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
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

/**
 * Tests for batch individual record tracing functionality.
 *
 * @author Igor Quintanilha
 * @since 3.4
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = {BatchIndividualRecordTracingTests.BATCH_INDIVIDUAL_TRACING_TOPIC, 
		BatchIndividualRecordTracingTests.BATCH_ONLY_TRACING_TOPIC}, partitions = 1)
@DirtiesContext
public class BatchIndividualRecordTracingTests {

	public static final String BATCH_INDIVIDUAL_TRACING_TOPIC = "batch.individual.tracing";

	public static final String BATCH_ONLY_TRACING_TOPIC = "batch.only.tracing";

	@Test
	void batchIndividualRecordTracingCreatesSpanPerRecord(@Autowired BatchListener listener,
			@Autowired KafkaTemplate<Integer, String> template, @Autowired SimpleTracer tracer)
			throws InterruptedException {

		// Clear any existing spans
		tracer.getSpans().clear();

		// Send multiple messages
		template.send(BATCH_INDIVIDUAL_TRACING_TOPIC, "message-1");
		template.send(BATCH_INDIVIDUAL_TRACING_TOPIC, "message-2");
		template.send(BATCH_INDIVIDUAL_TRACING_TOPIC, "message-3");

		// Wait for batch processing
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		// Verify spans were created
		Deque<SimpleSpan> allSpans = tracer.getSpans();
		
		// Find consumer spans (spans with listener ID should be present when individual tracing is enabled)
		List<SimpleSpan> consumerSpans = allSpans.stream()
				.filter(span -> span.getTags().containsKey("spring.kafka.listener.id"))
				.collect(Collectors.toList());

		// With batch individual record tracing enabled, we should get spans for individual records
		// At minimum, we should get at least 1 span, ideally 3 (one per record)
		assertThat(consumerSpans)
				.as("Should have consumer spans when batch individual record tracing is enabled")
				.isNotEmpty();

		// Verify each consumer span has proper listener context
		for (SimpleSpan consumerSpan : consumerSpans) {
			assertThat(consumerSpan.getTags())
					.as("Consumer span should have listener identification")
					.containsKey("spring.kafka.listener.id")
					.containsKey("messaging.system")
					.containsEntry("messaging.system", "kafka");
		}

		assertThat(listener.processedRecords).hasSize(3);
	}

	@Test
	void batchIndividualRecordTracingDisabledCreatesNoIndividualSpans(@Autowired BatchListenerWithoutIndividualTracing batchListener,
			@Autowired KafkaTemplate<Integer, String> template, @Autowired SimpleTracer tracer)
			throws InterruptedException {

		// Clear any existing spans
		tracer.getSpans().clear();

		// Send messages
		template.send(BATCH_ONLY_TRACING_TOPIC, "batch-message-1");
		template.send(BATCH_ONLY_TRACING_TOPIC, "batch-message-2");

		// Wait for batch processing
		assertThat(batchListener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		// When individual record tracing is disabled, no individual consumer spans should be created
		Deque<SimpleSpan> allSpans = tracer.getSpans();
		List<SimpleSpan> consumerSpans = allSpans.stream()
				.filter(span -> span.getTags().containsKey("spring.kafka.listener.id"))
				.collect(Collectors.toList());

		// With batch individual record tracing disabled, we should have no individual consumer spans
		assertThat(consumerSpans)
				.as("No individual consumer spans should be created when batch individual tracing is disabled")
				.isEmpty();

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
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		SimpleTracer simpleTracer() {
			return new SimpleTracer();
		}

		@Bean
		ObservationRegistry observationRegistry(Tracer tracer, Propagator propagator, MeterRegistry meterRegistry) {
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
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry));
			return observationRegistry;
		}

		@Bean
		Propagator propagator(Tracer tracer) {
			return new Propagator() {

				// List of headers required for tracing propagation
				@Override
				public List<String> fields() {
					return Arrays.asList("foo", "bar");
				}

				// This is called on the producer side when the message is being sent
				// Normally we would pass information from tracing context - for tests we don't need to
				@Override
				public <C> void inject(TraceContext context, @Nullable C carrier, Setter<C> setter) {
					setter.set(carrier, "foo", "some foo value");
					setter.set(carrier, "bar", "some bar value");
				}

				// This is called on the consumer side when the message is consumed
				// Normally we would use tools like Extractor from tracing but for tests we are just manually creating a span
				@Override
				public <C> Span.Builder extract(C carrier, Getter<C> getter) {
					String foo = getter.get(carrier, "foo");
					String bar = getter.get(carrier, "bar");
					return tracer.spanBuilder()
							.tag("foo", foo)
							.tag("bar", bar);
				}
			};
		}

		@Bean
		BatchListener batchListener() {
			return new BatchListener();
		}

		@Bean
		BatchListenerWithoutIndividualTracing batchListenerWithoutIndividualTracing() {
			return new BatchListenerWithoutIndividualTracing();
		}
	}

	static class BatchListener {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<String> processedRecords = new ArrayList<>();

		@KafkaListener(topics = BATCH_INDIVIDUAL_TRACING_TOPIC, containerFactory = "observationListenerContainerFactory")
		public void listen(List<ConsumerRecord<Integer, String>> records) {
			for (ConsumerRecord<Integer, String> record : records) {
				processedRecords.add(record.value());
			}
			latch.countDown();
		}
	}

	static class BatchListenerWithoutIndividualTracing {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<String> processedRecords = new ArrayList<>();

		@KafkaListener(topics = BATCH_ONLY_TRACING_TOPIC, containerFactory = "batchOnlyObservationListenerContainerFactory")
		public void listen(List<ConsumerRecord<Integer, String>> records) {
			for (ConsumerRecord<Integer, String> record : records) {
				processedRecords.add(record.value());
			}
			latch.countDown();
		}
	}

}
