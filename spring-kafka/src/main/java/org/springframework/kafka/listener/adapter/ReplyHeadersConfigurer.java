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

package org.springframework.kafka.listener.adapter;

import java.util.Map;

import org.springframework.lang.Nullable;

/**
 * A strategy for configuring which headers, if any, should be set in a reply message.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
@FunctionalInterface
public interface ReplyHeadersConfigurer {

	/**
	 * Return true if the header should be copied to the reply message.
	 * {@link org.springframework.kafka.support.KafkaHeaders#CORRELATION_ID} will not be
	 * offered; it is always copied.
	 * {@link org.springframework.messaging.MessageHeaders#ID} and
	 * {@link org.springframework.messaging.MessageHeaders#TIMESTAMP} are never copied.
	 * {@code KafkaHeaders.RECEIVED*} headers are never copied.
	 * @param headerName the header name.
	 * @param headerValue the header value.
	 * @return true to copy.
	 */
	boolean shouldCopy(String headerName, Object headerValue);

	/**
	 * A map of additional headers to add to the reply message.
	 * IMPORTANT: Any existing headers with the same name will be replaced by those
	 * returned by this method.
	 * @return the headers.
	 */
	@Nullable
	default Map<String, Object> additionalHeaders() {
		return null;
	}

}
