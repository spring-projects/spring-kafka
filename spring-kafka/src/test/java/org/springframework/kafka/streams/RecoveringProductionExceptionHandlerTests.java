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

	private final ProducerRecord<byte[], byte[]> record;

	RecoveringProductionExceptionHandlerTests() {
		super(RecoveringProductionExceptionHandler.RECOVERER,
				RecoveringProductionExceptionHandler.DLQ_DESTINATION_RESOLVER);
		RecordHeaders headers = new RecordHeaders();
		headers.add("processor-header", "processor-header-value".getBytes());
		this.record = new ProducerRecord<>("source-topic", 2, 12345L,
				"key".getBytes(), "value".getBytes(), headers);
	}

	@Override
	protected RecoveringProductionExceptionHandler createHandler(Map<String, Object> configs) {
		RecoveringProductionExceptionHandler handler = new RecoveringProductionExceptionHandler();
		handler.configure(configs);
		return handler;
	}

	@Override
	protected ProductionExceptionHandler.Response handleError(
			RecoveringProductionExceptionHandler handler, ErrorHandlerContext context, Exception exception) {
		return handler.handleError(context, this.record, exception);
	}

	@Override
	protected ConsumerRecord<?, ?> createDestinationResolverConsumerRecord(ErrorHandlerContext context) {
		return new ConsumerRecord<>(
				context.topic(),
				context.partition(),
				context.offset(),
				this.record.timestamp(),
				TimestampType.NO_TIMESTAMP_TYPE,
				ConsumerRecord.NULL_SIZE,
				ConsumerRecord.NULL_SIZE,
				this.record.key(),
				this.record.value(),
				this.record.headers(),
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
				.satisfies(deadLetterRecord -> {
					assertThat(deadLetterRecord.topic()).isEqualTo(expectedRecord.topic());
					assertThat(deadLetterRecord.partition()).isEqualTo(expectedRecord.partition());
					assertThat(deadLetterRecord.key()).isEqualTo(expectedRecord.key());
					assertThat(deadLetterRecord.value()).isEqualTo(expectedRecord.value());
					assertThat(deadLetterRecord.headers().toArray().length).isEqualTo(9);
					expectedRecord.headers().forEach(expectedHeader ->
							assertThat(deadLetterRecord.headers().lastHeader(expectedHeader.key()))
									.isNotNull()
									.satisfies(h -> assertThat(h.value()).isEqualTo(expectedHeader.value())));
					// Do not validate content for the following headers, only presence
					assertThat(deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
					assertThat(deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isNotNull();
					assertThat(deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
				});
	}

}

