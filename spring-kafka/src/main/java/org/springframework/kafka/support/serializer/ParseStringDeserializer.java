/*
 * Copyright 2016-present the original author or authors.
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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * Generic {@link org.apache.kafka.common.serialization.Deserializer Deserializer} for deserialization of entity from
 * its {@link String} representation received from Kafka (a.k.a parsing).
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Alexei Klenin
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @since 2.5
 */
public class ParseStringDeserializer<T> implements Deserializer<T> {

	/**
	 * Property for the key parser method.
	 */
	public static final String KEY_PARSER = "spring.message.key.parser";

	/**
	 * Property for the key parser method.
	 */
	public static final String VALUE_PARSER = "spring.message.value.parser";

	private static final BiFunction<String, Headers, ?> NO_PARSER = (str, headers) -> {
		throw new IllegalStateException("A parser must be provided either via a constructor or consumer properties");
	};

	@SuppressWarnings("unchecked")
	private BiFunction<String, Headers, T> parser = (BiFunction<String, Headers, T>) NO_PARSER;

	private Charset charset = StandardCharsets.UTF_8;

	/**
	 * Construct an instance with no parser function; a static method name must be
	 * provided in the consumer config {@link #KEY_PARSER} or {@link #VALUE_PARSER}
	 * properties.
	 */
	public ParseStringDeserializer() {
	}

	/**
	 * Construct an instance with the supplied parser function. The function may receive
	 * null as the input value, for example for a tombstone record in a compacted topic.
	 * @param parser the function.
	 */
	public ParseStringDeserializer(Function<String, T> parser) {
		this.parser = (message, ignoredHeaders) -> parser.apply(message);
	}

	/**
	 * Construct an instance with the supplied parser function. The function may receive
	 * null as the input value, for example for a tombstone record in a compacted topic.
	 * @param parser the function.
	 */
	public ParseStringDeserializer(BiFunction<String, Headers, T> parser) {
		this.parser = parser;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (NO_PARSER.equals(this.parser)) {
			String parserMethod = (String) configs.get(isKey ? KEY_PARSER : VALUE_PARSER);
			Assert.state(parserMethod != null,
					"A parser must be provided either via a constructor or consumer properties");
			this.parser = SerializationUtils.propertyToMethodInvokingFunction(parserMethod, String.class,
					getClass().getClassLoader());
		}
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		return deserialize(topic, null, data);
	}

	@Override
	public T deserialize(String topic, @Nullable Headers headers, byte[] data) {
		return this.parser.apply(data == null ? null : new String(data, this.charset), headers);
	}

	@Override
	public T deserialize(String topic, Headers headers, ByteBuffer data) {
		String value = deserialize(data);
		return this.parser.apply(value, headers);
	}

	private @Nullable String deserialize(@Nullable ByteBuffer data) {
		if (data == null) {
			return null;
		}

		if (data.hasArray()) {
			return new String(data.array(), data.position() + data.arrayOffset(), data.remaining(), this.charset);
		}
		return new String(Utils.toArray(data), this.charset);
	}

	/**
	 * Set a charset to use when converting byte[] to {@link String}. Default UTF-8.
	 * @param charset  the charset.
	 */
	public void setCharset(Charset charset) {
		Assert.notNull(charset, "'charset' cannot be null");
		this.charset = charset;
	}

	/**
	 * Get the configured charset.
	 * @return the charset.
	 */
	public Charset getCharset() {
		return this.charset;
	}

	/**
	 * Get the configured parser function.
	 * @return the function.
	 */
	public BiFunction<String, Headers, T> getParser() {
		return this.parser;
	}

}
