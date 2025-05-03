/*
 * Copyright 2017-2025 the original author or authors.
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

package org.springframework.kafka.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.header.Header;

import org.springframework.kafka.retrytopic.RetryTopicHeaders;

/**
 * Extended Header Mapper based on {@link DefaultKafkaHeaderMapper}.
 * This Header Mapper manages header values as a list,
 * except for certain reserved headers.
 * Other behaviors are identical to {@link DefaultKafkaHeaderMapper}.
 *
 * @author Sanghyeok An
 *
 * @since 4.0.0
 *
 */
public class MultiValueKafkaHeaderMapper extends DefaultKafkaHeaderMapper {

	private final List<String> defaultSingleValueHeaderList = List.of(
			KafkaHeaders.PREFIX,
			KafkaHeaders.RECEIVED,
			KafkaHeaders.TOPIC,
			KafkaHeaders.KEY,
			KafkaHeaders.PARTITION,
			KafkaHeaders.OFFSET,
			KafkaHeaders.RAW_DATA,
			KafkaHeaders.RECORD_METADATA,
			KafkaHeaders.ACKNOWLEDGMENT,
			KafkaHeaders.CONSUMER,
			KafkaHeaders.RECEIVED_TOPIC,
			KafkaHeaders.RECEIVED_KEY,
			KafkaHeaders.RECEIVED_PARTITION,
			KafkaHeaders.TIMESTAMP_TYPE,
			KafkaHeaders.TIMESTAMP,
			KafkaHeaders.RECEIVED_TIMESTAMP,
			KafkaHeaders.NATIVE_HEADERS,
			KafkaHeaders.BATCH_CONVERTED_HEADERS,
			KafkaHeaders.CORRELATION_ID,
			KafkaHeaders.REPLY_TOPIC,
			KafkaHeaders.REPLY_PARTITION,
			KafkaHeaders.DLT_EXCEPTION_FQCN,
			KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN,
			KafkaHeaders.DLT_EXCEPTION_STACKTRACE,
			KafkaHeaders.DLT_EXCEPTION_MESSAGE,
			KafkaHeaders.DLT_KEY_EXCEPTION_STACKTRACE,
			KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE,
			KafkaHeaders.DLT_KEY_EXCEPTION_FQCN,
			KafkaHeaders.DLT_ORIGINAL_TOPIC,
			KafkaHeaders.DLT_ORIGINAL_PARTITION,
			KafkaHeaders.DLT_ORIGINAL_OFFSET,
			KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP,
			KafkaHeaders.DLT_ORIGINAL_TIMESTAMP,
			KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE,
			KafkaHeaders.GROUP_ID,
			KafkaHeaders.DELIVERY_ATTEMPT,
			KafkaHeaders.EXCEPTION_FQCN,
			KafkaHeaders.EXCEPTION_CAUSE_FQCN,
			KafkaHeaders.EXCEPTION_STACKTRACE,
			KafkaHeaders.EXCEPTION_MESSAGE,
			KafkaHeaders.KEY_EXCEPTION_STACKTRACE,
			KafkaHeaders.KEY_EXCEPTION_MESSAGE,
			KafkaHeaders.KEY_EXCEPTION_FQCN,
			KafkaHeaders.ORIGINAL_TOPIC,
			KafkaHeaders.ORIGINAL_PARTITION,
			KafkaHeaders.ORIGINAL_OFFSET,
			KafkaHeaders.ORIGINAL_TIMESTAMP,
			KafkaHeaders.ORIGINAL_TIMESTAMP_TYPE,
			KafkaHeaders.CONVERSION_FAILURES,
			KafkaHeaders.LISTENER_INFO,
			RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
			RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP,
			RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP);

	private final Set<String> singleValueHeaders = new HashSet<>(this.defaultSingleValueHeaderList);

	/**
	 * Adds headers that the {@link MultiValueKafkaHeaderMapper} should handle as single values.
	 * @param headerName the header name.
	 */
	public void addSingleValueHeader(String headerName) {
		this.singleValueHeaders.add(headerName);
	}

	@Override
	protected void handleHeader(String headerName, Header header, Map<String, Object> headers) {
		if (this.singleValueHeaders.contains(headerName)) {
			headers.put(headerName, headerValueToAddIn(header));
		}
		else {
			Object values = headers.getOrDefault(headerName, new ArrayList<>());

			if (values instanceof List) {
				@SuppressWarnings("unchecked")
				List<Object> castedValues = (List<Object>) values;
				castedValues.add(headerValueToAddIn(header));
				headers.put(headerName, castedValues);
			}
			else {
				headers.put(headerName, headerValueToAddIn(header));
			}

		}

	}

}
