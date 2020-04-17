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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Generic {@link org.apache.kafka.common.serialization.Deserializer Deserializer} for deserialization of entity from
 * its {@link String} representation received from Kafka (a.k.a parsing).
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Alexei Klenin
 * @author Gary Russell
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
	 * Construct an instance with the supplied parser function.
	 * @param parser the function.
	 */
	public ParseStringDeserializer(Function<String, T> parser) {
		this.parser = (message, ignoredHeaders) -> parser.apply(message);
	}

	/**
	 * Construct an instance with the supplied parser function.
	 * @param parser the function.
	 */
	public ParseStringDeserializer(BiFunction<String, Headers, T> parser) {
		this.parser = parser;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (NO_PARSER.equals(this.parser)) {
			String parserMethod = (String) configs.get(isKey ? KEY_PARSER : VALUE_PARSER);
			Assert.state(parserMethod != null,
					"A parser must be provided either via a constructor or consumer properties");
			int lastDotPosn = parserMethod.lastIndexOf(".");
			Assert.state(lastDotPosn > 1,
					"the parser method needs to be a class name followed by the method name, separated by '.'");
			Class<?> clazz;
			try {
				clazz = ClassUtils.forName(parserMethod.substring(0, lastDotPosn),
						getClass().getClassLoader());
			}
			catch (ClassNotFoundException | LinkageError e) {
				throw new IllegalStateException(e);
			}
			parserMethod = parserMethod.substring(lastDotPosn + 1);
			Method method;
			try {
				method = clazz.getDeclaredMethod(parserMethod, String.class, Headers.class);
			}
			catch (@SuppressWarnings("unused") NoSuchMethodException e) {
				try {
					method = clazz.getDeclaredMethod(parserMethod, String.class);
				}
				catch (NoSuchMethodException e1) {
					throw new IllegalStateException("the parser method must take '(String, Headers)' or '(String)'");
				}
				catch (SecurityException e1) {
					throw new IllegalStateException(e1);
				}
			}
			catch (SecurityException e) {
				throw new IllegalStateException(e);
			}
			Method parseMethod = method;
			if (method.getParameters().length > 1) {
				this.parser = (str, headers) -> {
					try {
						return (T) parseMethod.invoke(null, str, headers);
					}
					catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new IllegalStateException(e);
					}
				};
			}
			else {
				this.parser = (str, headers) -> {
					try {
						return (T) parseMethod.invoke(null, str);
					}
					catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new IllegalStateException(e);
					}
				};
			}
		}
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
