/*
 * Copyright 2022-present the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.handler.TracingAwareMeterObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTraceContext;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.micrometer.KafkaListenerObservation.DefaultKafkaListenerObservationConvention;
import org.springframework.kafka.support.micrometer.KafkaTemplateObservation.DefaultKafkaTemplateObservationConvention;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.StringUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Wang Zhiyang
 * @author Christian Mergenthaler
 * @author Soby Chacko
 * @author Francois Rosiere
 * @author Christian Fredriksson
 *
 * @since 3.0
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = {ObservationTests.OBSERVATION_TEST_1, ObservationTests.OBSERVATION_TEST_2,
		ObservationTests.OBSERVATION_TEST_3, ObservationTests.OBSERVATION_TEST_4, ObservationTests.OBSERVATION_REPLY,
		ObservationTests.OBSERVATION_RUNTIME_EXCEPTION, ObservationTests.OBSERVATION_ERROR,
		ObservationTests.OBSERVATION_TRACEPARENT_DUPLICATE, ObservationTests.OBSERVATION_ASYNC_FAILURE_TEST,
		ObservationTests.OBSERVATION_ASYNC_FAILURE_WITH_RETRY_TEST}, partitions = 1)
@DirtiesContext
public class ObservationTests {

	public final static String OBSERVATION_TEST_1 = "observation.testT1";

	public final static String OBSERVATION_TEST_2 = "observation.testT2";

	public final static String OBSERVATION_TEST_3 = "observation.testT3";

	public final static String OBSERVATION_TEST_4 = "observation.testT4";

	public final static String OBSERVATION_REPLY = "observation.reply";

	public final static String OBSERVATION_RUNTIME_EXCEPTION = "observation.runtime-exception";

	public final static String OBSERVATION_ERROR = "observation.error.sync";

	public final static String OBSERVATION_ERROR_COMPLETABLE_FUTURE = "observation.error.completableFuture";

	public final static String OBSERVATION_ERROR_MONO = "observation.error.mono";

	public final static String OBSERVATION_TRACEPARENT_DUPLICATE = "observation.traceparent.duplicate";

	public final static String OBSERVATION_ASYNC_FAILURE_TEST = "observation.async.failure.test";

	public final static String OBSERVATION_ASYNC_FAILURE_WITH_RETRY_TEST = "observation.async.failure.retry.test";

	@Test
	void asyncRetryScopePropagation(@Autowired AsyncFailureListener asyncFailureListener,
			@Autowired KafkaTemplate<Integer, String> template,
			@Autowired SimpleTracer tracer,
			@Autowired ObservationRegistry observationRegistry) throws InterruptedException {

		// Clear any previous spans
		tracer.getSpans().clear();

		// Create an observation scope to ensure we have a proper trace context
		var testObservation = Observation.createNotStarted("test.message.send", observationRegistry);

		// Send a message within the observation scope to ensure trace context is propagated
		testObservation.observe(() -> {
			try {
				template.send(OBSERVATION_ASYNC_FAILURE_TEST, "trigger-async-failure").get(5, TimeUnit.SECONDS);
			}
			catch (Exception e) {
				throw new RuntimeException("Failed to send message", e);
			}
		});

		// Wait for the listener to process the message (initial + retry + DLT = 3 invocations)
		assertThat(asyncFailureListener.asyncFailureLatch.await(100000, TimeUnit.SECONDS)).isTrue();

		// Verify that the captured spans from the listener contexts are all part of the same trace
		// This demonstrates that the tracing context propagates correctly through the retry mechanism
		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSizeGreaterThanOrEqualTo(4); // template + listener + retry + DLT spans

		// Verify that spans were captured for each phase and belong to the same trace
		assertThat(asyncFailureListener.capturedSpanInListener).isNotNull();
		assertThat(asyncFailureListener.capturedSpanInRetry).isNotNull();
		assertThat(asyncFailureListener.capturedSpanInDlt).isNotNull();

		// All spans should have the same trace ID, demonstrating trace continuity
		var originalTraceId = asyncFailureListener.capturedSpanInListener.getTraceId();
		assertThat(originalTraceId).isNotBlank();
		assertThat(asyncFailureListener.capturedSpanInRetry.getTraceId()).isEqualTo(originalTraceId);
		assertThat(asyncFailureListener.capturedSpanInDlt.getTraceId()).isEqualTo(originalTraceId);

		// Clear any previous spans
		tracer.getSpans().clear();
	}

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

		MessageListenerContainer listenerContainer1 = rler.getListenerContainer("obs1");
		listenerContainer1.stop();

		assertThat(template.send(OBSERVATION_TEST_1, "test")
				.thenAccept((sendResult) -> spanFromCallback.set(tracer.currentSpan())))
				.succeedsWithin(Duration.ofSeconds(20));

		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(1);

		SimpleSpan templateSpan = spans.peek();
		assertThat(templateSpan).isNotNull();
		assertThat(templateSpan.getTags()).containsAllEntriesOf(Map.of("key", "value"));

		assertThat(spanFromCallback.get()).isNotNull();
		listenerContainer1.start();
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
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, List.of(broker.getBrokersAsString()));
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
		if (keyValues.length > 0) {
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
								Map.entry("messaging.operation", "process"),
								Map.entry("messaging.source.kind", "topic"),
								Map.entry("messaging.source.name", sourceName),
								Map.entry("messaging.system", "kafka")));
		if (keyValues.length > 0) {
			Arrays.stream(keyValues).forEach(entry -> assertThat(span.getTags()).contains(entry));
		}
		assertThat(span.getName()).isEqualTo(sourceName + " process");
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
								"messaging.operation", "process",
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
			@Autowired KafkaListenerEndpointRegistry endpointRegistry,
			@Autowired MeterRegistry meterRegistry, @Autowired Config config)
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
		assertThat(span.getError())
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("obs4 run time exception");

		assertThat(meterRegistry.get("spring.kafka.listener")
				.tag("error", "IllegalStateException")
				.timer().count()).isEqualTo(1);

		assertThat(config.scopeInFailureReference.get()).isNotNull();
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

	@Test
	void observationErrorExceptionWhenCompletableFutureReturned(@Autowired ExceptionListener listener,
			@Autowired SimpleTracer tracer,
			@Autowired @Qualifier("throwableTemplate") KafkaTemplate<Integer, String> errorTemplate,
			@Autowired KafkaListenerEndpointRegistry endpointRegistry)
			throws ExecutionException, InterruptedException, TimeoutException {

		errorTemplate.send(OBSERVATION_ERROR_COMPLETABLE_FUTURE, "testError").get(10, TimeUnit.SECONDS);
		Deque<SimpleSpan> spans = tracer.getSpans();
		await().untilAsserted(() -> assertThat(spans).hasSize(2));
		SimpleSpan span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.template.name")).isEqualTo("throwableTemplate");
		span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.listener.id")).isEqualTo("obs6-0");
		assertThat(span.getError())
				.isInstanceOf(Error.class)
				.hasMessage("Should report metric.");
	}

	@Test
	void observationErrorExceptionWhenMonoReturned(@Autowired ExceptionListener listener, @Autowired SimpleTracer tracer,
			@Autowired @Qualifier("throwableTemplate") KafkaTemplate<Integer, String> errorTemplate,
			@Autowired KafkaListenerEndpointRegistry endpointRegistry)
			throws ExecutionException, InterruptedException, TimeoutException {

		errorTemplate.send(OBSERVATION_ERROR_MONO, "testError").get(10, TimeUnit.SECONDS);
		Deque<SimpleSpan> spans = tracer.getSpans();
		await().untilAsserted(() -> assertThat(spans).hasSize(2));
		SimpleSpan span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.template.name")).isEqualTo("throwableTemplate");
		span = spans.poll();
		assertThat(span.getTags().get("spring.kafka.listener.id")).isEqualTo("obs7-0");
		assertThat(span.getError())
				.isInstanceOf(Error.class)
				.hasMessage("Should report metric.");
	}

	@Test
	void kafkaAdminNotRecreatedIfBootstrapServersSameInProducerAndAdminConfig(
			@Autowired @Qualifier("reuseAdminBeanKafkaTemplate") KafkaTemplate<Integer, String> template,
			@Autowired KafkaAdmin kafkaAdmin) {
		// See this issue for more details: https://github.com/spring-projects/spring-kafka/issues/3466
		assertThat(template.getKafkaAdmin()).isSameAs(kafkaAdmin);
	}

	@Test
	void verifyKafkaRecordSenderContextTraceParentHandling() {
		String initialTraceParent = "traceparent-from-previous";
		String updatedTraceParent = "traceparent-current";
		ProducerRecord<Integer, String> record = new ProducerRecord<>("test-topic", "test-value");
		record.headers().add("traceparent", initialTraceParent.getBytes(StandardCharsets.UTF_8));

		// Create the context and update the traceparent
		KafkaRecordSenderContext context = new KafkaRecordSenderContext(
				record,
				"test-bean",
				() -> "test-cluster"
		);
		context.getSetter().set(record, "traceparent", updatedTraceParent);

		Iterable<Header> traceparentHeaders = record.headers().headers("traceparent");

		List<String> headerValues = StreamSupport.stream(traceparentHeaders.spliterator(), false)
				.map(header -> new String(header.value(), StandardCharsets.UTF_8))
				.toList();

		// Verify there's only one traceparent header and it contains the updated value
		assertThat(headerValues).containsExactly(updatedTraceParent);
	}

	@Test
	void verifyTraceParentHeader(@Autowired KafkaTemplate<Integer, String> template,
			@Autowired SimpleTracer tracer) throws Exception {
		CompletableFuture<ProducerRecord<Integer, String>> producerRecordFuture = new CompletableFuture<>();
		template.setProducerListener(new ProducerListener<>() {

			@Override
			public void onSuccess(ProducerRecord<Integer, String> producerRecord, RecordMetadata recordMetadata) {
				producerRecordFuture.complete(producerRecord);
			}
		});
		String initialTraceParent = "traceparent-from-previous";
		Header header = new RecordHeader("traceparent", initialTraceParent.getBytes(StandardCharsets.UTF_8));
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(
				OBSERVATION_TRACEPARENT_DUPLICATE,
				null, null, null,
				"test-value",
				List.of(header)
		);

		template.send(producerRecord).get(10, TimeUnit.SECONDS);
		ProducerRecord<Integer, String> recordResult = producerRecordFuture.get(10, TimeUnit.SECONDS);

		Iterable<Header> traceparentHeaders = recordResult.headers().headers("traceparent");
		assertThat(traceparentHeaders).hasSize(1);

		String traceparentValue = new String(traceparentHeaders.iterator().next().value(), StandardCharsets.UTF_8);
		assertThat(traceparentValue).isEqualTo("traceparent-from-propagator");

		tracer.getSpans().clear();
	}

	@Test
	void testReplyingKafkaTemplateObservation(
			@Autowired ReplyingKafkaTemplate<Integer, String, String> template,
			@Autowired ObservationRegistry observationRegistry) {
		assertThat(template.sendAndReceive(new ProducerRecord<>(OBSERVATION_TEST_4, "test"))
				// the current observation must be retrieved from the consumer thread of the reply
				.thenApply(replyRecord -> observationRegistry.getCurrentObservation().getContext()))
				.isCompletedWithValueMatchingWithin(observationContext ->
						observationContext instanceof KafkaRecordReceiverContext
								&& "spring.kafka.listener".equals(observationContext.getName()), Duration.ofSeconds(30));
	}

	@Configuration
	@EnableKafka
	public static class Config {

		KafkaAdmin mockAdmin = mock(KafkaAdmin.class);

		AtomicReference<Observation.Scope> scopeInFailureReference = new AtomicReference<>();

		@Bean
		KafkaAdmin admin(EmbeddedKafkaBroker broker) {
			String[] brokers = StringUtils.commaDelimitedListToStringArray(broker.getBrokersAsString());
			List<String> brokersAsList = Arrays.asList(brokers);
			KafkaAdmin admin = new KafkaAdmin(
					Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokersAsList));
			admin.setOperationTimeout(42);
			return admin;
		}

		@Bean
		@Primary
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
			producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString() + ","
					+ broker.getBrokersAsString());
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		ProducerFactory<Integer, String> customProducerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
			producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(broker, "obs", false);
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
		KafkaTemplate<Integer, String> reuseAdminBeanKafkaTemplate(
				@Qualifier("customProducerFactory") ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		ReplyingKafkaTemplate<Integer, String, String> replyingKafkaTemplate(
				ProducerFactory<Integer, String> pf,
				ConcurrentKafkaListenerContainerFactory<Integer, String> containerFactory) {

			var kafkaTemplate = new ReplyingKafkaTemplate<>(pf, containerFactory.createContainer(OBSERVATION_REPLY));
			kafkaTemplate.setObservationEnabled(true);
			return kafkaTemplate;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf, ObservationRegistry observationRegistry,
				KafkaTemplate<Integer, String> kafkaTemplate) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.setReplyTemplate(kafkaTemplate);
			factory.getContainerProperties().setObservationEnabled(true);
			factory.setContainerCustomizer(container -> {
				if (container.getListenerId().equals("obs3")) {
					container.setKafkaAdmin(this.mockAdmin);
				}
				if (container.getListenerId().contains("asyncFailure")) {
					// Enable async acks to trigger async failure handling
					container.getContainerProperties().setAsyncAcks(true);
					container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
				}
				if (container.getListenerId().equals("obs4")) {
					container.setRecordInterceptor(new RecordInterceptor<>() {

						@Override
						public ConsumerRecord<Integer, String> intercept(ConsumerRecord<Integer, String> record,
								Consumer<Integer, String> consumer) {

							return record;
						}

						@Override
						public void failure(ConsumerRecord<Integer, String> record, Exception exception,
								Consumer<Integer, String> consumer) {

							Config.this.scopeInFailureReference.set(observationRegistry.getCurrentObservationScope());
						}
					});
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
			var observationRegistry = ObservationRegistry.create();
			observationRegistry.observationConfig().observationHandler(
							// Composite will pick the first matching handler
							new ObservationHandler.FirstMatchingCompositeObservationHandler(
									// This is responsible for creating a span on the receiver side
									new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
									// This is responsible for creating a child span on the sender side
									new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
									// This is responsible for creating a default span
									new DefaultTracingObservationHandler(tracer)))
					.observationHandler(new TracingAwareMeterObservationHandler<>(
							new DefaultMeterObservationHandler(meterRegistry), tracer));
			return observationRegistry;
		}

		@Bean
		Propagator propagator(Tracer tracer) {
			return new Propagator() {

				// List of headers required for tracing propagation
				@Override
				public List<String> fields() {
					return Arrays.asList("traceId", "spanId", "foo", "bar");
				}

				// This is called on the producer side when the message is being sent
				@Override
				public <C> void inject(TraceContext context, @Nullable C carrier, Setter<C> setter) {
					setter.set(carrier, "foo", "some foo value");
					setter.set(carrier, "bar", "some bar value");

					setter.set(carrier, "traceId", context.traceId());
					setter.set(carrier, "spanId", context.spanId());

					// Add a traceparent header to simulate W3C trace context
					setter.set(carrier, "traceparent", "traceparent-from-propagator");
				}

				// This is called on the consumer side when the message is consumed
				@Override
				public <C> Span.Builder extract(C carrier, Getter<C> getter) {
					String foo = getter.get(carrier, "foo");
					String bar = getter.get(carrier, "bar");

					var traceId = getter.get(carrier, "traceId");
					var spanId = getter.get(carrier, "spanId");

					Span.Builder spanBuilder = tracer.spanBuilder()
							.tag("foo", foo)
							.tag("bar", bar);

					var traceContext = new SimpleTraceContext();
					traceContext.setTraceId(traceId);
					traceContext.setSpanId(spanId);
					spanBuilder = spanBuilder.setParent(traceContext);

					return spanBuilder;
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

		@Bean
		AsyncFailureListener asyncFailureListener(SimpleTracer tracer) {
			return new AsyncFailureListener(tracer);
		}

		@Bean
		public TaskScheduler taskExecutor() {
			return new ThreadPoolTaskScheduler();
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

		@KafkaListener(id = "obsReply", topics = OBSERVATION_TEST_4)
		@SendTo  // default REPLY_TOPIC header
		public String replyListener(ConsumerRecord<Integer, String> in) {
			return in.value().toUpperCase();
		}

	}

	public static class ExceptionListener {

		final CountDownLatch latch4 = new CountDownLatch(1);

		final CountDownLatch latch5 = new CountDownLatch(1);

		@KafkaListener(id = "obs4", topics = OBSERVATION_RUNTIME_EXCEPTION)
		void listenRuntimeException(ConsumerRecord<Integer, String> in) {
			try {
				throw new IllegalStateException("obs4 run time exception");
			}
			finally {
				this.latch4.countDown();
			}
		}

		@KafkaListener(id = "obs5", topics = OBSERVATION_ERROR)
		void listenError(ConsumerRecord<Integer, String> in) {
			try {
				throw new Error("obs5 error");
			}
			finally {
				this.latch5.countDown();
			}
		}

		@KafkaListener(id = "obs6", topics = OBSERVATION_ERROR_COMPLETABLE_FUTURE)
		CompletableFuture<Void> receive(ConsumerRecord<Object, Object> record) {
			return CompletableFuture.supplyAsync(() -> {
				throw new Error("Should report metric.");
			});
		}

		@KafkaListener(id = "obs7", topics = OBSERVATION_ERROR_MONO)
		Mono<Void> receive1(ConsumerRecord<Object, Object> record) {
			return Mono.error(new Error("Should report metric."));
		}

	}

	public static class AsyncFailureListener {

		final CountDownLatch asyncFailureLatch = new CountDownLatch(3);

		volatile @Nullable SimpleSpan capturedSpanInListener;

		volatile @Nullable SimpleSpan capturedSpanInRetry;

		volatile @Nullable SimpleSpan capturedSpanInDlt;

		private final SimpleTracer tracer;

		public AsyncFailureListener(SimpleTracer tracer) {
			this.tracer = tracer;
		}

		@RetryableTopic(
				attempts = "2",
				backOff = @BackOff(delay = 1000)
		)
		@KafkaListener(id = "asyncFailure", topics = OBSERVATION_ASYNC_FAILURE_TEST)
		CompletableFuture<Void> handleAsync(ConsumerRecord<Integer, String> record) {

			// Use topic name to distinguish between original and retry calls
			String topicName = record.topic();

			if (topicName.equals(OBSERVATION_ASYNC_FAILURE_TEST)) {
				// This is the original call
				this.capturedSpanInListener = this.tracer.currentSpan();
			}
			else {
				// This is a retry call (topic name will be different for retry topics)
				this.capturedSpanInRetry = this.tracer.currentSpan();
			}

			this.asyncFailureLatch.countDown();

			// Return a failed CompletableFuture to trigger async failure handling
			return CompletableFuture.supplyAsync(() -> {
				throw new RuntimeException("Async failure for observation test");
			});
		}

		@DltHandler
		void handleDlt(ConsumerRecord<Integer, String> record, Exception exception) {
			this.capturedSpanInDlt = this.tracer.currentSpan();
			this.asyncFailureLatch.countDown();
		}

	}

}
