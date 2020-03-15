/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.kafka.support.serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.util.Assert;

/**
 * Generic {@link org.apache.kafka.common.serialization.Deserializer Deserializer} for deserialization of entity from
 * its {@link String} representation received from Kafka (a.k.a parsing).
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Alexei Klenin
 * @since 2.5
 */
public class ParseStringDeserializer<T> implements Deserializer<T> {

	private final BiFunction<String, Headers, T> parser;
	protected Charset charset = StandardCharsets.UTF_8;

	public ParseStringDeserializer(Function<String, T> parser) {
		this.parser = (message, ignoredHeaders) -> parser.apply(message);
	}

	public ParseStringDeserializer(BiFunction<String, Headers, T> parser) {
		this.parser = parser;
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		return deserialize(topic, null, data);
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		return this.parser.apply(new String(data, this.charset), headers);
	}

	/**
	 * Set a charset to use when converting byte[] to {@link String}. Default UTF-8.
	 * @param charset  the charset.
	 */
	public void setCharset(Charset charset) {
		Assert.notNull(charset, "'charset' cannot be null");
		this.charset = charset;
	}

}
