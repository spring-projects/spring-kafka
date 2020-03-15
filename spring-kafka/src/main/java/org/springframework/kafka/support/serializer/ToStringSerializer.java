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
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Generic {@link org.apache.kafka.common.serialization.Serializer Serializer} that relies on
 * {@link Object#toString()} to get serialized representation of the entity.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Alexei Klenin
 * @since 2.5
 */
public class ToStringSerializer<T> implements Serializer<T> {

	/**
	 * Kafka config property for enabling/disabling adding type headers.
	 */
	public static final String ADD_TYPE_INFO_HEADERS = "spring.message.add.type.headers";


	/**
	 * Header for the type of key.
	 */
	public static final String KEY_TYPE = "spring.message.key.type";

	/**
	 * Header for the type of value.
	 */
	public static final String VALUE_TYPE = "spring.message.value.type";

	protected boolean addTypeInfo = true;
	protected Charset charset = StandardCharsets.UTF_8;
	private String typeInfoHeader = VALUE_TYPE;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (isKey) {
			this.typeInfoHeader = KEY_TYPE;
		}

		if (configs.containsKey(ADD_TYPE_INFO_HEADERS)) {
			Object config = configs.get(ADD_TYPE_INFO_HEADERS);
			if (config instanceof Boolean) {
				this.addTypeInfo = (Boolean) config;
			}
			else if (config instanceof String) {
				this.addTypeInfo = Boolean.parseBoolean((String) config);
			}
			else {
				throw new IllegalStateException(
						ADD_TYPE_INFO_HEADERS + " must be Boolean or String");
			}
		}
	}

	@Override
	public byte[] serialize(String topic, @Nullable T data) {
		return serialize(topic, null, data);
	}

	@Override
	@Nullable
	public byte[] serialize(String topic, @Nullable Headers headers, @Nullable T data) {
		if (data == null) {
			return null;
		}

		if (this.addTypeInfo && headers != null) {
			headers.add(this.typeInfoHeader, data.getClass().getName().getBytes());
		}

		return data.toString().getBytes(this.charset);
	}

	@Override
	public void close() {
		// No-op
	}

	public boolean isAddTypeInfo() {
		return this.addTypeInfo;
	}

	/**
	 * Set to false to disable adding type info headers.
	 * @param addTypeInfo  true to add headers
	 */
	public void setAddTypeInfo(boolean addTypeInfo) {
		this.addTypeInfo = addTypeInfo;
	}

	/**
	 * Set a charset to use when converting {@link String} to byte[]. Default UTF-8.
	 * @param charset  the charset.
	 */
	public void setCharset(Charset charset) {
		Assert.notNull(charset, "'charset' cannot be null");
		this.charset = charset;
	}

}
