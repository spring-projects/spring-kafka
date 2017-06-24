/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.HeaderMapper;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class DefaultKafkaHeaderMapper implements HeaderMapper<Headers> {

	private static final Log logger = LogFactory.getLog(DefaultKafkaHeaderMapper.class);

	private static final String JSON_TYPES = "spring_json_header_types";

	private final ObjectMapper objectMapper;

	public DefaultKafkaHeaderMapper() {
		this(new ObjectMapper());
	}

	public DefaultKafkaHeaderMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public void fromHeaders(MessageHeaders headers, Headers target) {
		final Map<String, String> jsonHeaders = new HashMap<>();
		headers.forEach((k, v) -> {
			if (v instanceof byte[]) {
				target.add(new RecordHeader(k, (byte[]) v));
			}
			else {
				try {
					target.add(new RecordHeader(k, this.objectMapper.writeValueAsBytes(v)));
					jsonHeaders.put(k, v.getClass().getName());
				}
				catch (Exception e) {
					if (logger.isDebugEnabled()) {
						logger.debug("Could not map " + k + " with type " + v.getClass().getName());
					}
				}
			}
		});
		if (jsonHeaders.size() > 0) {
			try {
				target.add(new RecordHeader(JSON_TYPES, this.objectMapper.writeValueAsBytes(jsonHeaders)));
			}
			catch (IllegalStateException | JsonProcessingException e) {
				logger.error("Could not add json types header", e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public MessageHeaders toHeaders(Headers source) {
		final Map<String, Object> headers = new HashMap<String, Object>();
		Map<String, String> types = null;
		Iterator<Header> iterator = source.iterator();
		while (iterator.hasNext()) {
			Header next = iterator.next();
			if (next.key().equals(JSON_TYPES)) {
				try {
					types = this.objectMapper.readValue(next.value(), HashMap.class);
				}
				catch (IOException e) {
					logger.error("Could not decode json types: " + new String(next.value()), e);
				}
			}
		}
		final Map<String, String> jsonTypes = types;
		source.forEach(h -> {
			if (!(h.key().equals(JSON_TYPES))) {
				if (jsonTypes != null && jsonTypes.containsKey(h.key())) {
					Class<?> type = Object.class;
					try {
						type = ClassUtils.forName(jsonTypes.get(h.key()), null);
					}
					catch (Exception e) {
						logger.error("Could not load class for header: " + h.key(), e);
					}
					try {
						headers.put(h.key(), this.objectMapper.readValue(h.value(), type));
					}
					catch (IOException e) {
						logger.error("Could not decode json type: " + new String(h.value()) + " for key: " + h.key(),
								e);
						headers.put(h.key(), h.value());
					}
				}
				else {
					headers.put(h.key(), h.value());
				}
			}
		});
		return new MessageHeaders(headers);
	}

}
