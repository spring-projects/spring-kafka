/*
 * Copyright 2020 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.lang.Nullable;

/**
 * Utilities for listener adapters.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public final class AdapterUtils {

	/**
	 * Parser context for runtime SpEL using ! as the template prefix.
	 * @since 2.2.15
	 */
	public static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

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
		if (!(data instanceof ConsumerRecord)) {
			return null;
		}
		ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) data;
		return new ConsumerRecordMetadata(new RecordMetadata(new TopicPartition(record.topic(), record.partition()),
				0, record.offset(), record.timestamp(), null, record.serializedKeySize(),
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

}
