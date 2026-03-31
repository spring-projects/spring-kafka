/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.kafka.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Abstract base test class for {@link RecoveringDeserializationExceptionHandlerTests},
 * {@link RecoveringProcessingExceptionHandlerTests} and {@link RecoveringProductionExceptionHandlerTests} implementations.
 *
 * @param <H> the handler type
 * @param <R> the handler-specific response type
 * @author Loïc Greffier
 * @since 4.1
 */
abstract class AbstractRecoveringExceptionHandlerTests<H, R> {

	private final String recovererPropertyKey;

	private final String destinationResolverPropertyKey;

	protected AbstractRecoveringExceptionHandlerTests(String recovererPropertyKey,
			String destinationResolverPropertyKey) {
		this.recovererPropertyKey = recovererPropertyKey;
		this.destinationResolverPropertyKey = destinationResolverPropertyKey;
	}

	protected abstract H createHandler(Map<String, Object> configs);

	protected abstract R handleError(H handler, ErrorHandlerContext context, Exception exception);

	protected abstract void assertResponseShouldResume(R response);

	protected abstract void assertResponseShouldFail(R response);

	protected abstract void assertResponseShouldContainDeadLetterRecords(R response,
			ProducerRecord<byte[], byte[]> expectedRecord);

	protected ErrorHandlerContext createMockContext() {
		ErrorHandlerContext context = mock(ErrorHandlerContext.class);
		given(context.topic()).willReturn("foo");
		given(context.partition()).willReturn(0);
		given(context.offset()).willReturn(0L);
		given(context.timestamp()).willReturn(0L);
		given(context.sourceRawKey()).willReturn(new byte[0]);
		given(context.sourceRawValue()).willReturn(new byte[0]);
		given(context.headers()).willReturn(new RecordHeaders());
		return context;
	}

	@Test
	void withRecovererAsStringProperty() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(this.recovererPropertyKey, Recoverer.class.getName());
		H handler = createHandler(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isInstanceOf(Recoverer.class);
		assertResponseShouldResume(handleError(handler, createMockContext(), new IllegalArgumentException()));
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalStateException()));
	}

	@Test
	void withRecovererAsClassProperty() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(this.recovererPropertyKey, Recoverer.class);
		H handler = createHandler(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isInstanceOf(Recoverer.class);
		assertResponseShouldResume(handleError(handler, createMockContext(), new IllegalArgumentException()));
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalStateException()));
	}

	@Test
	void withRecovererAsObjectProperty() {
		Map<String, Object> configs = new HashMap<>();
		Recoverer rec = new Recoverer();
		configs.put(this.recovererPropertyKey, rec);
		H handler = createHandler(configs);
		assertThat(KafkaTestUtils.getPropertyValue(handler, "recoverer")).isSameAs(rec);
		assertResponseShouldResume(handleError(handler, createMockContext(), new IllegalArgumentException()));
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalStateException()));
	}

	@Test
	void withNoRecoverer() {
		H handler = createHandler(new HashMap<>());
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalArgumentException()));
	}

	@Test
	void withDestinationResolverAsStringProperty() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(this.destinationResolverPropertyKey, KafkaStreamsDlqDestinationResolver.class.getName());
		H handler = createHandler(configs);
		ErrorHandlerContext context = createMockContext();
		R result = handleError(handler, context, new IllegalArgumentException());
		assertThat(KafkaTestUtils.getPropertyValue(handler, "destinationResolver"))
				.isInstanceOf(KafkaStreamsDlqDestinationResolver.class);
		assertResponseShouldResume(result);
		assertResponseShouldContainDeadLetterRecords(result,
				new ProducerRecord<>("foo", 1, new byte[0], new byte[0]));
	}

	@Test
	void withDestinationResolverAsClassProperty() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(this.destinationResolverPropertyKey, KafkaStreamsDlqDestinationResolver.class);
		H handler = createHandler(configs);
		ErrorHandlerContext context = createMockContext();
		R result = handleError(handler, context, new IllegalArgumentException());
		assertThat(KafkaTestUtils.getPropertyValue(handler, "destinationResolver"))
				.isInstanceOf(KafkaStreamsDlqDestinationResolver.class);
		assertResponseShouldResume(result);
		assertResponseShouldContainDeadLetterRecords(result,
				new ProducerRecord<>("foo", 1, new byte[0], new byte[0]));
	}

	@Test
	void withDestinationResolverAsObjectProperty() {
		Map<String, Object> configs = new HashMap<>();
		KafkaStreamsDlqDestinationResolver resolver = new KafkaStreamsDlqDestinationResolver();
		configs.put(this.destinationResolverPropertyKey, resolver);
		H handler = createHandler(configs);
		ErrorHandlerContext context = createMockContext();
		R result = handleError(handler, context, new IllegalArgumentException());
		assertThat(KafkaTestUtils.getPropertyValue(handler, "destinationResolver")).isSameAs(resolver);
		assertResponseShouldResume(result);
		assertResponseShouldContainDeadLetterRecords(result,
				new ProducerRecord<>("foo", 1, new byte[0], new byte[0]));
	}

	@Test
	void withDeadLetterQueueTopic() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "foo");
		H handler = createHandler(configs);
		ErrorHandlerContext context = createMockContext();
		R result = handleError(handler, context, new IllegalArgumentException());
		assertResponseShouldResume(result);
		assertResponseShouldContainDeadLetterRecords(result,
				new ProducerRecord<>("foo", null, new byte[0], new byte[0]));
	}

	@Test
	void withNullDeadLetterQueueTopicShouldFail() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, null);
		H handler = createHandler(configs);
		assertResponseShouldFail(handleError(handler, createMockContext(), new IllegalArgumentException()));
	}

	public static class Recoverer implements ConsumerRecordRecoverer {

		@Override
		public void accept(ConsumerRecord<?, ?> record, Exception exception) {
			if (exception instanceof IllegalStateException) {
				throw new IllegalStateException("recovery failed test");
			}
		}

	}

	public static class KafkaStreamsDlqDestinationResolver implements KafkaStreamsDeadLetterDestinationResolver {

		@Override
		public @NonNull TopicPartition resolve(@NonNull ErrorHandlerContext errorHandlerContext, @NonNull ConsumerRecord<?, ?> record, @NonNull Exception exception) {
			return new TopicPartition("foo", 1);
		}

	}

}

