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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;

/**
 * A {@link DeserializationExceptionHandler} that calls a {@link ConsumerRecordRecoverer}
 * or uses the native Kafka Streams DLQ and continues.
 *
 * @author Gary Russell
 * @author Soby Chacko
 * @since 2.3
 *
 */
public class RecoveringDeserializationExceptionHandler
		extends AbstractRecoveringExceptionHandler<DeserializationExceptionHandler.Response>
		implements DeserializationExceptionHandler {

	/**
	 * Property name for configuring the recoverer using properties.
	 * @deprecated Since 4.1 in favor of {@link #RECOVERER}.
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	public static final String KSTREAM_DESERIALIZATION_RECOVERER = "spring.deserialization.recoverer";

	/**
	 * Property name for configuring the recoverer using properties.
	 */
	public static final String RECOVERER = "spring.kafka.streams.deserialization.exception.handler.recoverer";

	/**
	 * Property name for configuring the native DLQ destination resolver.
	 */
	public static final String DLQ_DESTINATION_RESOLVER = "spring.kafka.streams.deserialization.exception.handler.dlq.destination.resolver";

	public RecoveringDeserializationExceptionHandler() {
	}

	public RecoveringDeserializationExceptionHandler(ConsumerRecordRecoverer recoverer) {
		super(recoverer);
	}

	/**
	 * Handle the deserialization exception by delegating to the configured recoverer.
	 * @deprecated since 4.1 in favor of {@link #handleError(ErrorHandlerContext, ConsumerRecord, Exception)}.
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	@Override
	@SuppressWarnings("deprecation")
	public DeserializationHandlerResponse handle(ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record,
			Exception exception) {

		Response response = handleError(context, record, exception);
		return response.result() == Result.RESUME
				? DeserializationHandlerResponse.CONTINUE
				: DeserializationHandlerResponse.FAIL;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Response handleError(ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record,
			Exception exception) {
		return handleErrorCommon(context, record, exception, record);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Map<String, ?> configs) {
		configureCommon(configs, DLQ_DESTINATION_RESOLVER, RECOVERER, KSTREAM_DESERIALIZATION_RECOVERER);
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
