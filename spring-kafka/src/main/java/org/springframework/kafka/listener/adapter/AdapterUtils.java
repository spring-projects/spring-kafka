/*
 * Copyright 2020-present the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.ClassUtils;

/**
 * Utilities for listener adapters.
 *
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Huijin Hong
 * @since 2.5
 *
 */
public final class AdapterUtils {

	/**
	 * Parser context for runtime SpEL using ! as the template prefix.
	 * @since 2.2.15
	 */
	public static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	private static final boolean MONO_PRESENT =
			ClassUtils.isPresent("reactor.core.publisher.Mono", AdapterUtils.class.getClassLoader());

	private AdapterUtils() {
	}

	/**
	 * Build a {@link ConsumerRecordMetadata} from the first {@link ConsumerRecord} in data, if any.
	 * @param data the data array.
	 * @return the metadata or null if data does not contain a {@link ConsumerRecord}.
	 */
	@Nullable
	public static Object buildConsumerRecordMetadataFromArray(Object... data) {
		for (Object object : data) {
			ConsumerRecordMetadata metadata = buildConsumerRecordMetadata(object);
			if (metadata != null) {
				return metadata;
			}
		}
		return null;
	}

	/**
	 * Build a {@link ConsumerRecordMetadata} from data which must be a
	 * {@link ConsumerRecord}.
	 * @param data the record.
	 * @return the metadata or null if data is not a {@link ConsumerRecord}.
	 */
	@Nullable
	public static ConsumerRecordMetadata buildConsumerRecordMetadata(Object data) {
		if (!(data instanceof ConsumerRecord<?, ?> record)) {
			return null;
		}
		return new ConsumerRecordMetadata(new RecordMetadata(new TopicPartition(record.topic(), record.partition()),
				record.offset(), 0, record.timestamp(), record.serializedKeySize(),
				record.serializedValueSize()), record.timestampType());
	}

	/**
	 * Return the default expression when no SendTo value is present.
	 * @return the expression.
	 * @since 2.2.15
	 */
	public static String getDefaultReplyTopicExpression() {
		return PARSER_CONTEXT.getExpressionPrefix() + "source.headers['"
				+ KafkaHeaders.REPLY_TOPIC + "']" + PARSER_CONTEXT.getExpressionSuffix();
	}

	/**
	 * Return the true when return types are asynchronous.
	 * @param  resultType {@code InvocableHandlerMethod} return type.
	 * @return type is {@code Mono} or {@code CompletableFuture}.
	 * @since 3.2
	 */
	public static boolean isAsyncReply(Class<?> resultType) {
		return isMono(resultType) || isCompletableFuture(resultType);
	}

	/**
	 * Return the true when type is {@code Mono}.
	 * @param  resultType {@code InvocableHandlerMethod} return type.
	 * @return type is {@code Mono}.
	 * @since 3.2
	 */
	public static boolean isMono(Class<?> resultType) {
		return MONO_PRESENT && Mono.class.isAssignableFrom(resultType);
	}

	/**
	 * Return the true when type is {@code CompletableFuture}.
	 * @param  resultType {@code InvocableHandlerMethod} return type.
	 * @return type is {@code CompletableFuture}.
	 * @since 3.2
	 */
	public static boolean isCompletableFuture(Class<?> resultType) {
		return CompletableFuture.class.isAssignableFrom(resultType);
	}

	/**
	 * Return the true when type is {@code Continuation}.
	 * @param  parameterType {@code MethodParameter} parameter type.
	 * @return type is {@code Continuation}.
	 * @since 3.2.1
	 */
	public static boolean isKotlinContinuation(Class<?> parameterType) {
		return "kotlin.coroutines.Continuation".equals(parameterType.getName());
	}
}
