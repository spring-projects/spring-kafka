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

import java.nio.ByteBuffer;
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
import org.springframework.kafka.support.KafkaHeaders;
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

	protected abstract ConsumerRecord<?, ?> createDestinationResolverConsumerRecord(ErrorHandlerContext context);

	protected ErrorHandlerContext createMockContext() {
		ErrorHandlerContext context = mock(ErrorHandlerContext.class);
		given(context.topic()).willReturn("source-topic");
		given(context.partition()).willReturn(2);
		given(context.offset()).willReturn(42L);
		given(context.timestamp()).willReturn(150L);
		given(context.sourceRawKey()).willReturn(new byte[0]);
		given(context.sourceRawValue()).willReturn(new byte[0]);
		RecordHeaders headers = new RecordHeaders();
		headers.add("original-header", "original-header-value".getBytes());
		given(context.headers()).willReturn(headers);
		return context;
	}

	private ProducerRecord<byte[], byte[]> createExpectedDeadLetterRecord(Integer partition, ErrorHandlerContext context) {
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("dlq-topic", partition, context.timestamp(), context.sourceRawKey(), context.sourceRawValue(), context.headers());
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, context.topic().getBytes());
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(Integer.BYTES).putInt(context.partition()).array());
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(context.offset()).array());
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP, ByteBuffer.allocate(Long.BYTES).putLong(context.timestamp()).array());
		return record;
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
		assertResponseShouldContainDeadLetterRecords(result, createExpectedDeadLetterRecord(1, context));
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
		assertResponseShouldContainDeadLetterRecords(result, createExpectedDeadLetterRecord(1, context));
	}

	@Test
	void withDestinationResolverAsObjectProperty() {
		Map<String, Object> configs = new HashMap<>();
		ErrorHandlerContext context = createMockContext();
		ConsumerRecord<?, ?> expectedRecord = createDestinationResolverConsumerRecord(context);
		KafkaStreamsDestinationResolverWithAsserts resolver = new KafkaStreamsDestinationResolverWithAsserts(expectedRecord);
		configs.put(this.destinationResolverPropertyKey, resolver);
		H handler = createHandler(configs);
		R result = handleError(handler, context, new IllegalArgumentException());
		assertThat(KafkaTestUtils.getPropertyValue(handler, "destinationResolver")).isSameAs(resolver);
		assertResponseShouldResume(result);
		assertResponseShouldContainDeadLetterRecords(result, createExpectedDeadLetterRecord(1, context));
	}

	@Test
	void withDeadLetterQueueTopic() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq-topic");
		H handler = createHandler(configs);
		ErrorHandlerContext context = createMockContext();
		R result = handleError(handler, context, new IllegalArgumentException());
		assertResponseShouldResume(result);
		assertResponseShouldContainDeadLetterRecords(result, createExpectedDeadLetterRecord(null, context));
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
			return new TopicPartition("dlq-topic", 1);
		}

	}

	public static class KafkaStreamsDestinationResolverWithAsserts implements KafkaStreamsDeadLetterDestinationResolver {

		private final ConsumerRecord<?, ?> expectedRecord;

		KafkaStreamsDestinationResolverWithAsserts(ConsumerRecord<?, ?> expectedRecord) {
			this.expectedRecord = expectedRecord;
		}

		@Override
		public @NonNull TopicPartition resolve(@NonNull ErrorHandlerContext errorHandlerContext, @NonNull ConsumerRecord<?, ?> record, @NonNull Exception exception) {
			assertThat(record.topic()).isEqualTo(this.expectedRecord.topic());
			assertThat(record.partition()).isEqualTo(this.expectedRecord.partition());
			assertThat(record.offset()).isEqualTo(this.expectedRecord.offset());
			assertThat(record.timestamp()).isEqualTo(this.expectedRecord.timestamp());
			assertThat(record.key()).isEqualTo(this.expectedRecord.key());
			assertThat(record.value()).isEqualTo(this.expectedRecord.value());
			assertThat(record.headers()).isEqualTo(this.expectedRecord.headers());
			return new TopicPartition("dlq-topic", 1);
		}

	}

}

