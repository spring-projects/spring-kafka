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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterRecordManager;
import org.springframework.kafka.listener.NativeDeadLetterDestinationResolver;
import org.springframework.util.ClassUtils;

/**
 * Abstract base class for recovering Kafka Streams exception handlers.
 *
 * @param <R> the handler-specific response type
 * @author Loïc Greffier
 * @since 4.1
 */
public abstract class AbstractRecoveringExceptionHandler<R> {

	private static final Log LOGGER = LogFactory.getLog(AbstractRecoveringExceptionHandler.class);

	protected final DeadLetterRecordManager deadLetterRecordManager = new DeadLetterRecordManager();

	protected @Nullable ConsumerRecordRecoverer recoverer;

	protected @Nullable NativeDeadLetterDestinationResolver destinationResolver;

	protected @Nullable String deadLetterTopic;

	protected AbstractRecoveringExceptionHandler() {
	}

	protected AbstractRecoveringExceptionHandler(ConsumerRecordRecoverer recoverer) {
		this.recoverer = recoverer;
	}

	/**
	 * Common error handling logic.
	 * @param context the error handler context
	 * @param record the consumer record that caused the error
	 * @param exception the exception that occurred
	 * @return a handler-specific response
	 */
	protected R handleErrorCommon(ErrorHandlerContext context, ConsumerRecord<?, ?> record, Exception exception) {
		if (this.destinationResolver != null) {
			TopicPartition tp = this.destinationResolver.apply(context, record, exception);
			ProducerRecord<byte[], byte[]> outRecord = this.deadLetterRecordManager.enrichHeadersAndCreateProducerRecord(
					record, exception, tp, context.sourceRawKey(), context.sourceRawValue());
			return resume(Collections.singletonList(outRecord));
		}

		if (this.deadLetterTopic != null) {
			ProducerRecord<byte[], byte[]> outRecord = this.deadLetterRecordManager.enrichHeadersAndCreateProducerRecord(
					record, exception, new TopicPartition(this.deadLetterTopic, -1), context.sourceRawKey(), context.sourceRawValue());
			return resume(Collections.singletonList(outRecord));
		}

		if (this.recoverer == null) {
			return fail();
		}
		try {
			this.recoverer.accept(record, exception);
			return resume(Collections.emptyList());
		}
		catch (RuntimeException e) {
			LOGGER.error("Recoverer threw an exception; recovery failed", e);
			return fail();
		}
	}

	/**
	 * Configure common attributes.
	 * @param configs the configuration map
	 * @param destinationResolverKey the property key for the native DLQ destination resolver
	 * @param recovererKey the property key for the recoverer
	 */
	protected void configureCommon(Map<String, ?> configs, String destinationResolverKey, String recovererKey) {
		if (configs.containsKey(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG)) {
			this.deadLetterTopic = String.valueOf(configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG));
		}

		if (configs.containsKey(destinationResolverKey)) {
			Object configValue = configs.get(destinationResolverKey);
			if (configValue instanceof NativeDeadLetterDestinationResolver) {
				this.destinationResolver = (NativeDeadLetterDestinationResolver) configValue;
			}
			else if (configValue instanceof String) {
				this.destinationResolver = fromString(configValue, NativeDeadLetterDestinationResolver.class);
			}
			else if (configValue instanceof Class) {
				this.destinationResolver = fromClass(configValue, NativeDeadLetterDestinationResolver.class);
			}
			else {
				LOGGER.error("Unknown property type for " + destinationResolverKey
						+ "; failed operations cannot be resolved to a DLT destination");
			}
		}

		if (configs.containsKey(recovererKey)) {
			Object configValue = configs.get(recovererKey);
			if (configValue instanceof ConsumerRecordRecoverer) {
				this.recoverer = (ConsumerRecordRecoverer) configValue;
			}
			else if (configValue instanceof String) {
				this.recoverer = fromString(configValue, ConsumerRecordRecoverer.class);
			}
			else if (configValue instanceof Class) {
				this.recoverer = fromClass(configValue, ConsumerRecordRecoverer.class);
			}
			else {
				LOGGER.error("Unknown property type for " + recovererKey
						+ "; failed operations cannot be recovered");
			}
		}
	}

	private <T> @Nullable T fromString(Object configValue, Class<T> targetType) throws LinkageError {
		try {
			Class<?> clazz = ClassUtils.forName((String) configValue,
					AbstractRecoveringExceptionHandler.class.getClassLoader());
			Constructor<?> constructor = clazz.getConstructor();
			return targetType.cast(constructor.newInstance());
		}
		catch (Exception e) {
			LOGGER.error("Failed to instantiate " + targetType.getSimpleName() + " from class name " + configValue, e);
			return null;
		}
	}

	private <T> @Nullable T fromClass(Object configValue, Class<T> targetType) {
		try {
			Class<?> clazz = (Class<?>) configValue;
			Constructor<?> constructor = clazz.getConstructor();
			return targetType.cast(constructor.newInstance());
		}
		catch (Exception e) {
			LOGGER.error("Failed to instantiate " + targetType.getSimpleName() + " from class " + ((Class<?>) configValue).getName(), e);
			return null;
		}
	}

	/**
	 * Create a response indicating that processing should fail.
	 * @return a handler-specific fail response
	 */
	protected abstract R fail();

	/**
	 * Create a response indicating that processing should resume.
	 * @param deadLetterRecords The list of dead letter records to forward to DLQ
	 * @return a handler-specific resume response
	 */
	protected abstract R resume(List<ProducerRecord<byte[], byte[]>> deadLetterRecords);
}

