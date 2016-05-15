/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.kafka.support.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Generic {@link Serializer} for sending Java objects to Kafka as JSON.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 */
public class JsonSerializer<T> implements Serializer<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerializer.class);

	private ObjectWriter writer;

	public void configure(Map<String, ?> configs, boolean isKey) {
		LOGGER.debug("Start configuring");
		this.writer = JsonDatabindFactory.createSerializer(configs, isKey);
		LOGGER.debug("Finish configuring");
	}

	public byte[] serialize(String topic, T data) {
		try {
			LOGGER.debug("Start processing");
			byte[] result = null;
			if (data != null) {
				result = this.writer.writeValueAsBytes(data);
			}
			LOGGER.debug("Finish processing");
			return result;
		}
		catch (JsonProcessingException ex) {
			LOGGER.debug("Failed processing");
			throw new JsonWrapperException(ex);
		}
	}

	public void close() {
		LOGGER.debug("Nothing to close");
	}
}
