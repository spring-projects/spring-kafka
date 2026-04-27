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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;

/**
 * A {@link ProcessingExceptionHandler} that calls a {@link ConsumerRecordRecoverer}
 * or uses the native Kafka Streams DLQ and continues.
 *
 * @author Loïc Greffier
 * @since 4.1
 */
public class RecoveringProcessingExceptionHandler
		extends AbstractRecoveringExceptionHandler<ProcessingExceptionHandler.Response>
		implements ProcessingExceptionHandler {

	/**
	 * Property name for configuring the recoverer using properties.
	 */
	public static final String RECOVERER = "spring.kafka.streams.processing.exception.handler.recoverer";

	/**
	 * Property name for configuring the native DLQ destination resolver.
	 */
	public static final String DLQ_DESTINATION_RESOLVER = "spring.kafka.streams.processing.exception.handler.dlq.destination.resolver";

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Response handleError(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {
		ConsumerRecord<byte[], byte[]> sourceRecord = new ConsumerRecord<>(
				context.topic(),
				context.partition(),
				context.offset(),
				context.timestamp(),
				TimestampType.NO_TIMESTAMP_TYPE,
				context.sourceRawKey().length,
				context.sourceRawValue().length,
				context.sourceRawKey(),
				context.sourceRawValue(),
				context.headers(),
				Optional.empty(),
				Optional.empty());

		ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>(
				context.topic(),
				context.partition(),
				context.offset(),
				record.timestamp(),
				TimestampType.NO_TIMESTAMP_TYPE,
				ConsumerRecord.NULL_SIZE,
				ConsumerRecord.NULL_SIZE,
				record.key(),
				record.value(),
				record.headers(),
				Optional.empty(),
				Optional.empty());

		return handleErrorCommon(context, consumerRecord, exception, sourceRecord);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Map<String, ?> configs) {
		configureCommon(configs, DLQ_DESTINATION_RESOLVER, RECOVERER, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Response fail() {
		return Response.fail();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Response resume(List<ProducerRecord<byte[], byte[]>> deadLetterRecords) {
		return Response.resume(deadLetterRecords);
	}
}
