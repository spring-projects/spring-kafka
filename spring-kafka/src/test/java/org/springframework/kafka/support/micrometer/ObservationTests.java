/*
 * Copyright 2022-2024 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.micrometer.KafkaListenerObservation.DefaultKafkaListenerObservationConvention;
import org.springframework.kafka.support.micrometer.KafkaTemplateObservation.DefaultKafkaTemplateObservationConvention;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Wang Zhiyang
 * @author Christian Mergenthaler
 * @author Soby Chacko
 *
 * @since 3.0
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = { ObservationTests.OBSERVATION_TEST_1, ObservationTests.OBSERVATION_TEST_2,
		ObservationTests.OBSERVATION_TEST_3, ObservationTests.OBSERVATION_RUNTIME_EXCEPTION,
		ObservationTests.OBSERVATION_ERROR }, partitions = 1)
@DirtiesContext
public class ObservationTests {

	public final static String OBSERVATION_TEST_1 = "observation.testT1";

	public final static String OBSERVATION_TEST_2 = "observation.testT2";

	public final static String OBSERVATION_TEST_3 = "observation.testT3";

	public final static String OBSERVATION_RUNTIME_EXCEPTION = "observation.runtime-exception";

	public final static String OBSERVATION_ERROR = "observation.error";

	@Test
	void endToEnd(@Autowired Listener listener, @Autowired KafkaTemplate<Integer, String> template,
			@Autowired SimpleTracer tracer, @Autowired KafkaListenerEndpointRegistry rler,
			@Autowired MeterRegistry meterRegistry, @Autowired EmbeddedKafkaBroker broker,
			@Autowired KafkaListenerEndpointRegistry endpointRegistry, @Autowired KafkaAdmin admin,
			@Autowired @Qualifier("customTemplate") KafkaTemplate<Integer, String> customTemplate,
			@Autowired Config config)
					throws InterruptedException, ExecutionException, TimeoutException {

		AtomicReference<SimpleSpan> spanFromCallback = new AtomicReference<>();

		template.setProducerInterceptor(new ProducerInterceptor<>() {
			@Override
			public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
				tracer.currentSpanCustomizer().tag("key", "value");
				return record;
			}

			@Override
			public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

			}

			@Override
			public void close() {

			}

			@Override
			public void configure(Map<String, ?> configs) {

			}
		});

		template.send(OBSERVATION_TEST_1, "test")
				.thenAccept((sendResult) -> spanFromCallback.set(tracer.currentSpan()))
				.get(10, TimeUnit.SECONDS);

		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(1);

		SimpleSpan templateSpan = spans.peek();
		assertThat(templateSpan).isNotNull();
		assertThat(templateSpan.getTags()).containsAllEntriesOf(Map.of(
				"key", "value"));

		assertThat(spanFromCallback.get()).isNotNull();
		MessageListenerContainer listenerContainer1 = rler.getListenerContainer("obs1");
		MessageListenerContainer listenerContainer2 = rler.getListenerContainer("obs2");
		assertThat(listenerContainer1).isNotNull();
		assertThat(listenerContainer2).isNotNull();
		// consumer factory broker different to admin
		assertThatContainerAdmin(listenerContainer1, admin,
				broker.getBrokersAsString() + "," + broker.getBrokersAsString() + ","
						+ broker.getBrokersAsString());
		// broker override in annotation
		assertThatContainerAdmin(listenerContainer2, admin, broker.getBrokersAsString());

		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		listenerContainer1.stop();
		listenerContainer2.stop();

		assertThat(listener.record).isNotNull();
		Headers headers = listener.record.headers();
		assertThat(headers.lastHeader("foo")).extracting(Header::value).isEqualTo("some foo value".getBytes());
		assertThat(headers.lastHeader("bar")).extracting(Header::value).isEqualTo("some bar value".getBytes());
		spans = tracer.getSpans();
		assertThat(spans).hasSize(4);
		assertThatTemplateSpanTags(spans, 6, OBSERVATION_TEST_1);
		assertThatListenerSpanTags(spans, 12, OBSERVATION_TEST_1, "obs1-0", "obs1", "0", "0");
		assertThatTemplateSpanTags(spans, 6, OBSERVATION_TEST_2);
		assertThatListenerSpanTags(spans, 12, OBSERVATION_TEST_2, "obs2-0", "obs2", "0", "0");
		template.setObservationConvention(new DefaultKafkaTemplateObservationConvention() {

			@Override
			public KeyValues getLowCardinalityKeyValues(KafkaRecordSenderContext context) {
				return super.getLowCardinalityKeyValues(context).and("foo", "bar");
			}

		});
		template.send(OBSERVATION_TEST_1, "test").get(10, TimeUnit.SECONDS);

		listenerContainer1.getContainerProperties().setObservationConvention(
				new DefaultKafkaListenerObservationConvention() {

					@Override
					public KeyValues getLowCardinalityKeyValues(KafkaRecordReceiverContext context) {
						return super.getLowCardinalityKeyValues(context).and("baz", "qux");
					}

				});

		listenerContainer2.start();
		listenerContainer1.start();
		assertThat(listener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		listenerContainer1.stop();
		listenerContainer2.stop();

		assertThat(listener.record).isNotNull();
		headers = listener.record.headers();
		assertThat(headers.lastHeader("foo")).extracting(Header::value).isEqualTo("some foo value".getBytes());
		assertThat(headers.lastHeader("bar")).extracting(Header::value).isEqualTo("some bar value".getBytes());
		assertThat(spans).hasSize(4);
		assertThatTemplateSpanTags(spans, 7, OBSERVATION_TEST_1, Map.entry("foo", "bar"));
		assertThatListenerSpanTags(spans, 13, OBSERVATION_TEST_1, "obs1-0", "obs1", "1", "0", Map.entry("baz", "qux"));
		assertThatTemplateSpanTags(spans, 7, OBSERVATION_TEST_2, Map.entry("foo", "bar"));
		SimpleSpan span = assertThatListenerSpanTags(spans, 12, OBSERVATION_TEST_2, "obs2-0", "obs2", "1", "0");
		assertThat(span.getTags()).doesNotContainEntry("baz", "qux");
		MeterRegistryAssert meterRegistryAssert = MeterRegistryAssert.assertThat(meterRegistry);
		assertThatTemplateHasTimerWithNameAndTags(meterRegistryAssert, OBSERVATION_TEST_1);
		assertThatListenerHasTimerWithNameAndTags(meterRegistryAssert, OBSERVATION_TEST_1, "obs1", "obs1-0");
		assertThatTemplateHasTimerWithNameAndTags(meterRegistryAssert, OBSERVATION_TEST_2, "foo", "bar");
		assertThatListenerHasTimerWithNameAndTags(meterRegistryAssert, OBSERVATION_TEST_1, "obs1", "obs1-0",
				"baz", "qux");
		assertThatListenerHasTimerWithNameAndTags(meterRegistryAssert, OBSERVATION_TEST_2, "obs2", "obs2-0");

		assertThat(admin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		// producer factory broker different to admin
		assertThatAdmin(template, admin, broker.getBrokersAsString() + "," + broker.getBrokersAsString(),
				"kafkaAdmin");
		// custom admin
		assertThat(customTemplate.getKafkaAdmin()).isSameAs(config.mockAdmin);

		// custom admin
		Object container = KafkaTestUtils.getPropertyValue(
				endpointRegistry.getListenerContainer("obs3"), "containers", List.class).get(0);
		KafkaAdmin cAdmin = KafkaTestUtils.getPropertyValue(
				container, "listenerConsumer.kafkaAdmin", KafkaAdmin.class);
		assertThat(cAdmin).isSameAs(config.mockAdmin);

		assertThatExceptionOfType(KafkaException.class)
				.isThrownBy(() -> template.send("wrong%Topic", "data"))
				.withCauseExactlyInstanceOf(InvalidTopicException.class);

		MeterRegistryAssert.assertThat(meterRegistry)
				.hasTimerWithNameAndTags("spring.kafka.template", KeyValues.of("error", "InvalidTopicException"))
				.doesNotHaveMeterWithNameAndTags("spring.kafka.template", KeyValues.of("error", "KafkaException"));
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	private void assertThatTemplateSpanTags(Deque<SimpleSpan> spans, int tagSize, String destName,
			Map.Entry<String, String>... keyValues) {

		SimpleSpan span = spans.poll();
		assertThat(span).isNotNull();
		await().until(() -> span.getTags().size() == tagSize);
		assertThat(span.getTags()).containsAllEntriesOf(Map.of(
				"spring.kafka.template.name", "template",
				"messaging.operation", "publish",
				"messaging.system", "kafka",
				"messaging.destination.kind", "topic",
				"messaging.destination.name", destName));
		if (keyValues != null && keyValues.length > 0) {
			Arrays.stream(keyValues).forEach(entry -> assertThat(span.getTags()).contains(entry));
		}
		assertThat(span.getName()).isEqualTo(destName + " send");
		assertThat(span.getRemoteServiceName()).startsWith("Apache Kafka: ");
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	private SimpleSpan assertThatListenerSpanTags(Deque<SimpleSpan> spans, int tagSize, String sourceName,
			String listenerId, String consumerGroup, String offset, String partition,
			Map.Entry<String, String>... keyValues) {

		SimpleSpan span = spans.poll();
		assertThat(span).isNotNull();
		await().until(() -> span.getTags().size() == tagSize);
		String clientId = span.getTags().get("messaging.kafka.client_id");
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.ofEntries(Map.entry("spring.kafka.listener.id", listenerId),
								Map.entry("foo", "some foo value"),
								Map.entry("bar", "some bar value"),
								Map.entry("messaging.consumer.id", consumerGroup + " - " + clientId),
								Map.entry("messaging.kafka.consumer.group", consumerGroup),
								Map.entry("messaging.kafka.message.offset", offset),
								Map.entry("messaging.kafka.source.partition", partition),
								Map.entry("messaging.operation", "receive"),
								Map.entry("messaging.source.kind", "topic"),
								Map.entry("messaging.source.name", sourceName),
								Map.entry("messaging.system", "kafka")));
		if (keyValues != null && keyValues.length > 0) {
			Arrays.stream(keyValues).forEach(entry -> assertThat(span.getTags()).contains(entry));
		}
		assertThat(span.getName()).isEqualTo(sourceName + " receive");
		return span;
	}

	private void assertThatTemplateHasTimerWithNameAndTags(MeterRegistryAssert meterRegistryAssert, String destName,
			String... keyValues) {

		meterRegistryAssert.hasTimerWithNameAndTags("spring.kafka.template",
				KeyValues.of("spring.kafka.template.name", "template",
						"messaging.operation", "publish",
						"messaging.system", "kafka",
						"messaging.destination.kind", "topic",
						"messaging.destination.name", destName)
						.and(keyValues));
	}

	private void assertThatListenerHasTimerWithNameAndTags(MeterRegistryAssert meterRegistryAssert, String destName,
			String consumerGroup, String listenerId, String... keyValues) {

		meterRegistryAssert.hasTimerWithNameAndTags("spring.kafka.listener",
				KeyValues.of(
						"messaging.kafka.consumer.group", consumerGroup,
						"messaging.operation", "receive",
						"messaging.source.kind", "topic",
						"messaging.source.name", destName,
						"messaging.system", "kafka",
						"spring.kafka.listener.id", listenerId)
						.and(keyValues));
	}

	private void assertThatContainerAdmin(MessageListenerContainer listenerContainer, KafkaAdmin admin,
			String brokersString) {

		Object container = KafkaTestUtils.getPropertyValue(listenerContainer, "containers", List.class).get(0);
		assertThatAdmin(container, admin, brokersString, "listenerConsumer.kafkaAdmin");
	}

	private void assertThatAdmin(Object object, KafkaAdmin admin, String brokersString, String key) {
		KafkaAdmin cAdmin = KafkaTestUtils.getPropertyValue(object, key, KafkaAdmin.class);
		assertThat(cAdmin.getOperationTimeout()).isEqualTo(admin.getOperationTimeout());
		assertThat(cAdmin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokersString);
	}

	@Test
	void observationRuntimeException(@Autowired ExceptionListener listener, @Autowired SimpleTracer tracer,
			@Autowired @Qualifier("throwableTemplate") KafkaTemplate<Integer, String> runtimeExceptionTemplate,
			@Autowired KafkaListenerEndpointRegistry endpointRegistry)
					throws ExecutionException, InterruptedException, TimeoutException {

		runtimeExceptionTemplate.send(OBSERVATION_RUNTIME_EXCEPTION, "testRuntimeException").get(10, TimeUnit.SECONDS);
		assertThat(listener.latch4.await(10, TimeUnit.SECONDS)).isTrue();
		endpointRegistry.getListenerContainer("obs4").stop();

		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(2);
		SimpleSpan span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.template.name")).isEqualTo("throwableTemplate");
		span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.listener.id")).isEqualTo("obs4-0");
		assertThat(span.getError().getCause())
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("obs4 run time exception");
	}

	@Test
	void observationErrorException(@Autowired ExceptionListener listener, @Autowired SimpleTracer tracer,
			@Autowired @Qualifier("throwableTemplate") KafkaTemplate<Integer, String> errorTemplate,
			@Autowired KafkaListenerEndpointRegistry endpointRegistry)
					throws ExecutionException, InterruptedException, TimeoutException {

		errorTemplate.send(OBSERVATION_ERROR, "testError").get(10, TimeUnit.SECONDS);
		assertThat(listener.latch5.await(10, TimeUnit.SECONDS)).isTrue();
		endpointRegistry.getListenerContainer("obs5").stop();

		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(2);
		SimpleSpan span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.template.name")).isEqualTo("throwableTemplate");
		span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.listener.id")).isEqualTo("obs5-0");
		assertThat(span.getError())
				.isInstanceOf(Error.class)
				.hasMessage("obs5 error");
	}

	@Configuration
	@EnableKafka
	public static class Config {

		KafkaAdmin mockAdmin = mock(KafkaAdmin.class);

		@Bean
		KafkaAdmin admin(EmbeddedKafkaBroker broker) {
			KafkaAdmin admin = new KafkaAdmin(
					Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
			admin.setOperationTimeout(42);
			return admin;
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
			producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString() + ","
					+ broker.getBrokersAsString());
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("obs", "false", broker);
			consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString() + ","
					+ broker.getBrokersAsString() + "," + broker.getBrokersAsString());
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		@Primary
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		KafkaTemplate<Integer, String> customTemplate(ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			template.setKafkaAdmin(this.mockAdmin);
			return template;
		}

		@Bean
		KafkaTemplate<Integer, String> throwableTemplate(ProducerFactory<Integer, String> pf) {
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
			factory.getContainerProperties().setObservationEnabled(true);
			factory.setContainerCustomizer(container -> {
				if (container.getListenerId().equals("obs3")) {
					container.setKafkaAdmin(this.mockAdmin);
				}
			});
			return factory;
		}

		@Bean
		SimpleTracer simpleTracer() {
			return new SimpleTracer();
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		ObservationRegistry observationRegistry(Tracer tracer, Propagator propagator, MeterRegistry meterRegistry) {
			TestObservationRegistry observationRegistry = TestObservationRegistry.create();
			observationRegistry.observationConfig().observationHandler(
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
					return tracer.spanBuilder().tag("foo", foo).tag("bar", bar);
				}
			};
		}

		@Bean
		Listener listener(KafkaTemplate<Integer, String> template) {
			return new Listener(template);
		}

		@Bean
		ExceptionListener exceptionListener() {
			return new ExceptionListener();
		}

	}

	public static class Listener {

		private final KafkaTemplate<Integer, String> template;

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(2);

		volatile ConsumerRecord<?, ?> record;

		public Listener(KafkaTemplate<Integer, String> template) {
			this.template = template;
		}

		@KafkaListener(id = "obs1", topics = OBSERVATION_TEST_1)
		void listen1(ConsumerRecord<Integer, String> in) {
			this.template.send(OBSERVATION_TEST_2, in.value());
		}

		@KafkaListener(id = "obs2", topics = OBSERVATION_TEST_2,
				properties = ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + ":" + "#{@embeddedKafka.brokersAsString}")
		void listen2(ConsumerRecord<?, ?> in) {
			this.record = in;
			this.latch1.countDown();
			this.latch2.countDown();
		}

		@KafkaListener(id = "obs3", topics = OBSERVATION_TEST_3)
		void listen3(ConsumerRecord<Integer, String> in) {
		}

	}

	public static class ExceptionListener {

		final CountDownLatch latch4 = new CountDownLatch(1);

		final CountDownLatch latch5 = new CountDownLatch(1);

		@KafkaListener(id = "obs4", topics = OBSERVATION_RUNTIME_EXCEPTION)
		void listenRuntimeException(ConsumerRecord<Integer, String> in) {
			this.latch4.countDown();
			throw new IllegalStateException("obs4 run time exception");
		}

		@KafkaListener(id = "obs5", topics = OBSERVATION_ERROR)
		void listenError(ConsumerRecord<Integer, String> in) {
			this.latch5.countDown();
			throw new Error("obs5 error");
		}

	}

}
