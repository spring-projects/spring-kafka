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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

import org.springframework.kafka.support.KafkaHeaders;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Loïc Greffier
 * @since 4.1
 */
class RecoveringProcessingExceptionHandlerTests
		extends AbstractRecoveringExceptionHandlerTests<RecoveringProcessingExceptionHandler,
		ProcessingExceptionHandler.Response> {

	RecoveringProcessingExceptionHandlerTests() {
		super(RecoveringProcessingExceptionHandler.RECOVERER,
				RecoveringProcessingExceptionHandler.DLQ_DESTINATION_RESOLVER);
	}

	@Override
	protected RecoveringProcessingExceptionHandler createHandler(Map<String, Object> configs) {
		RecoveringProcessingExceptionHandler handler = new RecoveringProcessingExceptionHandler();
		handler.configure(configs);
		return handler;
	}

	@Override
	protected ProcessingExceptionHandler.Response handleError(
			RecoveringProcessingExceptionHandler handler, ErrorHandlerContext context, Exception exception) {
		return handler.handleError(context,
				new Record<>(context.sourceRawKey(), context.sourceRawValue(), context.timestamp()), exception);
	}

	@Override
	protected void assertResponseShouldResume(ProcessingExceptionHandler.Response response) {
		assertThat(response.result()).isEqualTo(ProcessingExceptionHandler.Result.RESUME);
	}

	@Override
	protected void assertResponseShouldFail(ProcessingExceptionHandler.Response response) {
		assertThat(response.result()).isEqualTo(ProcessingExceptionHandler.Result.FAIL);
	}

	@Override
	protected void assertResponseShouldContainDeadLetterRecords(
			ProcessingExceptionHandler.Response response, ProducerRecord<byte[], byte[]> expectedRecord) {
		assertThat(response.deadLetterQueueRecords()).hasSize(1).first()
				.satisfies(record -> {
					assertThat(record.topic()).isEqualTo(expectedRecord.topic());
					assertThat(record.partition()).isEqualTo(expectedRecord.partition());
					assertThat(record.key()).isEqualTo(expectedRecord.key());
					assertThat(record.value()).isEqualTo(expectedRecord.value());
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
					assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE).value()).isNotNull();
				});
	}

}

