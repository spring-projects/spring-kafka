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
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import org.springframework.kafka.support.KafkaHeaders;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Loïc Greffier
 * @since 4.1
 */
class RecoveringProductionExceptionHandlerTests
		extends AbstractRecoveringExceptionHandlerTests<RecoveringProductionExceptionHandler,
		ProductionExceptionHandler.Response> {

	RecoveringProductionExceptionHandlerTests() {
		super(RecoveringProductionExceptionHandler.RECOVERER,
				RecoveringProductionExceptionHandler.DLQ_DESTINATION_RESOLVER);
	}

	@Override
	protected RecoveringProductionExceptionHandler createHandler(Map<String, Object> configs) {
		RecoveringProductionExceptionHandler handler = new RecoveringProductionExceptionHandler();
		handler.configure(configs);
		return handler;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected ProductionExceptionHandler.Response handleError(
			RecoveringProductionExceptionHandler handler, ErrorHandlerContext context, Exception exception) {
		ConsumerRecord<byte[], byte[]> consumerRecord = (ConsumerRecord<byte[], byte[]>) createExpectedConsumerRecord(context);
		return handler.handleError(context,
				new ProducerRecord<>(consumerRecord.topic(),
						consumerRecord.partition(),
						consumerRecord.timestamp(),
						consumerRecord.key(),
						consumerRecord.value(),
						consumerRecord.headers()),
				exception);
	}

	@Override
	protected ConsumerRecord<?, ?> createExpectedConsumerRecord(ErrorHandlerContext context) {
		RecordHeaders headers = new RecordHeaders();
		headers.add("custom-header", "custom-value".getBytes());
		return new ConsumerRecord<>(
				context.topic(),
				context.partition(),
				context.offset(),
				12345L,
				TimestampType.NO_TIMESTAMP_TYPE,
				ConsumerRecord.NULL_SIZE,
				ConsumerRecord.NULL_SIZE,
				"key".getBytes(),
				"value".getBytes(),
				headers,
				Optional.empty(),
				Optional.empty());
	}

	@Override
	protected void assertResponseShouldResume(ProductionExceptionHandler.Response response) {
		assertThat(response.result()).isEqualTo(ProductionExceptionHandler.Result.RESUME);
	}

	@Override
	protected void assertResponseShouldFail(ProductionExceptionHandler.Response response) {
		assertThat(response.result()).isEqualTo(ProductionExceptionHandler.Result.FAIL);
	}

	@Override
	protected void assertResponseShouldContainDeadLetterRecords(
			ProductionExceptionHandler.Response response, ProducerRecord<byte[], byte[]> expectedRecord) {
		assertThat(response.deadLetterQueueRecords()).hasSize(1).first()
				.satisfies(record -> {
					assertThat(record.topic()).isEqualTo(expectedRecord.topic());
					assertThat(record.partition()).isEqualTo(expectedRecord.partition());
					assertThat(record.key()).isEqualTo(expectedRecord.key());
					assertThat(record.value()).isEqualTo(expectedRecord.value());
					assertThat(expectedRecord.headers()).hasSize(1);
					expectedRecord.headers().forEach(expectedHeader ->
							assertThat(record.headers().lastHeader(expectedHeader.key()))
									.isNotNull()
									.satisfies(h -> assertThat(h.value()).isEqualTo(expectedHeader.value())));
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull()
							.satisfies(h -> assertThat(new String(h.value())).isEqualTo(expectedRecord.topic()));
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull()
							.satisfies(h -> assertThat(ByteBuffer.wrap(h.value()).getInt()).isEqualTo(expectedRecord.partition()));
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).isNotNull()
							.satisfies(h -> assertThat(ByteBuffer.wrap(h.value()).getLong()).isEqualTo(expectedRecord.timestamp()));
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)).isNotNull();
				});
	}

}

