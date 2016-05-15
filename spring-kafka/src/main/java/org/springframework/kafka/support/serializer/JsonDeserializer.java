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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.ResolvableType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Generic {@link Deserializer} for sending Java objects to Kafka as JSON.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 */
public class JsonDeserializer<T> implements Deserializer<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonDeserializer.class);

	private ObjectReader reader;

	public void configure(Map<String, ?> configs, boolean isKey) {
		LOGGER.debug("Start configuring");
		Class<?> type = ResolvableType.forClass(this.getClass()).getSuperType().resolveGeneric(0);
		this.reader = JsonDatabindFactory.createDeserializer(type, configs, isKey);
		LOGGER.debug("Finish configuring");
	}

	public T deserialize(String topic, byte[] data) {
		try {
			LOGGER.debug("Start processing");
			T result = null;
			if (data != null) {
				result = this.reader.readValue(data);
			}
			LOGGER.debug("Finish processing");
			return result;
		}
		catch (JsonParseException ex) {
			LOGGER.error("Failed processing", ex);
			// if the exception is not processed, the same incorrect message is consumed in "infinite loop", so just logging the failure
			return null;
		}
		catch (IOException ex) {
			LOGGER.debug("Failed processing");
			throw new JsonWrapperException(ex);
		}
	}

	public void close() {
		LOGGER.debug("Nothing to close");
	}
}
