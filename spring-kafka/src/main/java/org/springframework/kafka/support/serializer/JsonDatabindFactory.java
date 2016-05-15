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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Main class for jackson JSON configuration.
 * TODO: customize jackson if necessary.
 *
 * @author Igor Stepanov
 */
public final class JsonDatabindFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonDatabindFactory.class);

	private static ObjectMapper MAPPER;
	private static ObjectWriter WRITER;

	static {
		// these classes are thread-safe, so using single instance per class is performance-efficient
		MAPPER = new ObjectMapper();
		MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		WRITER = MAPPER.writer();
	}

	/*
	 * Utility classes should not have a public or default constructor.
	 */
	private JsonDatabindFactory() {
	}

	/*
	 * Creates {@link ObjectMapper} for the specified config, returns static instance as of now.
	 *
	 * @param configs configs in key/value pairs
	 * @param isKey whether is for key or value
	 * @return technical entity to create ObjectReader/ObjectWriter
	 */
	private static ObjectMapper createMapper(Map<String, ?> configs, boolean isKey) {
		LOGGER.debug("Start configuring mapper");
		// TODO: config-based customization can be implemented here
		ObjectMapper result = MAPPER;
		LOGGER.debug("Finish configuring mapper");
		return result;
	}

	/**
	 * Creates {@link ObjectWriter} for the specified config, returns static instance as of now.
	 *
	 * @param configs configs in key/value pairs
	 * @param isKey whether is for key or value
	 * @return writer for sending Kafka messages
	 */
	public static ObjectWriter createSerializer(Map<String, ?> configs, boolean isKey) {
		LOGGER.debug("Start configuring writer");
		// TODO: config-based customization can be implemented here
		ObjectWriter result = WRITER;
		LOGGER.debug("Finish configuring writer");
		return result;
	}

	/**
	 * Creates {@link ObjectReader} for the specified class and config, ignores config as of now.
	 *
	 * @param type java class, used to parse messages into POJO
	 * @param configs configs in key/value pairs
	 * @param isKey whether is for key or value
	 * @return reader for parsing Kafka messages
	 */
	public static ObjectReader createDeserializer(Class<?> type, Map<String, ?> configs, boolean isKey) {
		LOGGER.debug("Start configuring reader");
		ObjectReader result = createMapper(configs, isKey).readerFor(type);
		LOGGER.debug("Finish configuring reader");
		return result;
	}
}
