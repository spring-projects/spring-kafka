/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.messaging.MessageHeaders;

/**
 * A simple header mapper that maps headers directly; for outbound,
 * only byte[] headers are mapped; for inbound, headers are mapped
 * unchanged, as byte[]. Strings can also be mapped to/from byte.
 * See {@link #setRawMappedHeaders(Map)}.
 * Most headers in {@link KafkaHeaders} are not mapped on outbound messages.
 * The exceptions are correlation and reply headers for request/reply
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public class SimpleKafkaHeaderMapper extends AbstractKafkaHeaderMapper {

	/**
	 * Construct an instance with the default object mapper and default header patterns
	 * for outbound headers; all inbound headers are mapped. The default pattern list is
	 * {@code "!id", "!timestamp" and "*"}. In addition, most of the headers in
	 * {@link KafkaHeaders} are never mapped as headers since they represent data in
	 * consumer/producer records.
	 */
	public SimpleKafkaHeaderMapper() {
		super("!" + MessageHeaders.ID,
				"!" + MessageHeaders.TIMESTAMP,
				"*");
	}

	/**
	 * Construct an instance with a default object mapper and the provided header patterns
	 * for outbound headers; all inbound headers are mapped. The patterns are applied in
	 * order, stopping on the first match (positive or negative). Patterns are negated by
	 * preceding them with "!". The patterns will replace the default patterns; you
	 * generally should not map the {@code "id" and "timestamp"} headers. Note:
	 * most of the headers in {@link KafkaHeaders} are never mapped as headers since they
	 * represent data in consumer/producer records.
	 * @param patterns the patterns.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public SimpleKafkaHeaderMapper(String... patterns) {
		super(patterns);
	}

	@Override
	public void fromHeaders(MessageHeaders headers, Headers target) {
		headers.forEach((key, value) -> {
			Object valueToAdd = headerValueToAddOut(key, value);
			if (valueToAdd instanceof byte[] && matches(key, valueToAdd)) {
				target.add(new RecordHeader(key, (byte[]) valueToAdd));
			}
		});
	}

	@Override
	public void toHeaders(Headers source, Map<String, Object> target) {
		source.forEach(header -> target.put(header.key(), headerValueToAddIn(header)));
	}

}
