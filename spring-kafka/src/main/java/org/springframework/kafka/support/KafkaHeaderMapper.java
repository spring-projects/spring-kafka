/*
 * Copyright 2017-2019 the original author or authors.
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

import org.springframework.messaging.MessageHeaders;

/**
 *  Header mapper for Apache Kafka.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public interface KafkaHeaderMapper {

	/**
	 * Map from the given {@link MessageHeaders} to the specified target message.
	 * @param headers the abstracted MessageHeaders.
	 * @param target the native target message.
	 */
	void fromHeaders(MessageHeaders headers, Headers target);

	/**
	 * Map from the given target message to abstracted {@link MessageHeaders}.
	 * @param source the native target message.
	 * @param target the target headers.
	 */
	void toHeaders(Headers source, Map<String, Object> target);

}
