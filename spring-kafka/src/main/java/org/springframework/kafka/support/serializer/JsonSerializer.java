/*
 * Copyright 2016-2018 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;

import org.springframework.kafka.support.converter.AbstractJavaTypeMapper;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Generic {@link org.apache.kafka.common.serialization.Serializer Serializer} for sending
 * Java objects to Kafka as JSON.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Gary Russell
 * @author Elliot Kennedy
 */
public class JsonSerializer<T> implements ExtendedSerializer<T> {

	/**
	 * Kafka config property for disabling adding type headers.
	 */
	public static final String ADD_TYPE_INFO_HEADERS = "spring.json.add.type.headers";

	/**
	 * Kafka config property to add type mappings to the type mapper:
	 * 'foo:com.Foo,bar:com.Bar'.
	 */
	public static final String TYPE_MAPPINGS = "spring.json.type.mapping";

	protected final ObjectMapper objectMapper; // NOSONAR

	protected boolean addTypeInfo = true; // NOSONAR

	protected Jackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper(); // NOSONAR

	private boolean typeMapperExplicitlySet = false;

	public JsonSerializer() {
		this(new ObjectMapper());
		this.objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public JsonSerializer(ObjectMapper objectMapper) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
	}

	public boolean isAddTypeInfo() {
		return this.addTypeInfo;
	}

	/**
	 * Set to false to disable adding type info headers.
	 * @param addTypeInfo true to add headers.
	 * @since 2.1
	 */
	public void setAddTypeInfo(boolean addTypeInfo) {
		this.addTypeInfo = addTypeInfo;
	}

	public Jackson2JavaTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Set a customized type mapper.
	 * @param typeMapper the type mapper.
	 * @since 2.1
	 */
	public void setTypeMapper(Jackson2JavaTypeMapper typeMapper) {
		Assert.notNull(typeMapper, "'typeMapper' cannot be null");
		this.typeMapper = typeMapper;
		this.typeMapperExplicitlySet = true;
	}

	/**
	 * Configure the default Jackson2JavaTypeMapper to use key type headers.
	 * @param isKey Use key type headers if true
	 * @since 2.1.3
	 */
	public void setUseTypeMapperForKey(boolean isKey) {
		if (!this.typeMapperExplicitlySet && getTypeMapper() instanceof AbstractJavaTypeMapper) {
			((AbstractJavaTypeMapper) getTypeMapper())
					.setUseForKey(isKey);
		}
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		setUseTypeMapperForKey(isKey);
		if (configs.containsKey(ADD_TYPE_INFO_HEADERS)) {
			Object config = configs.get(ADD_TYPE_INFO_HEADERS);
			if (config instanceof Boolean) {
				this.addTypeInfo = (Boolean) config;
			}
			else if (config instanceof String) {
				this.addTypeInfo = Boolean.valueOf((String) config);
			}
			else {
				throw new IllegalStateException(ADD_TYPE_INFO_HEADERS + " must be Boolean or String");
			}
		}
		if (configs.containsKey(TYPE_MAPPINGS) && !this.typeMapperExplicitlySet
				&& this.typeMapper instanceof AbstractJavaTypeMapper) {
			((AbstractJavaTypeMapper) this.typeMapper)
					.setIdClassMapping(createMappings((String) configs.get(TYPE_MAPPINGS)));
		}
	}

	protected static Map<String, Class<?>> createMappings(String mappings) {
		Map<String, Class<?>> mappingsMap = new HashMap<>();
		String[] array = StringUtils.commaDelimitedListToStringArray(mappings);
		for (String entry : array) {
			String[] split = entry.split(":");
			Assert.isTrue(split.length == 2, "Each comma-delimited mapping entry must have exactly one ':'");
			try {
				mappingsMap.put(split[0].trim(),
						ClassUtils.forName(split[1].trim(), ClassUtils.getDefaultClassLoader()));
			}
			catch (ClassNotFoundException | LinkageError e) {
				throw new IllegalArgumentException(e);
			}
		}
		return mappingsMap;
	}

	@Override
	@Nullable
	public byte[] serialize(String topic, Headers headers, @Nullable T data) {
		if (data == null) {
			return null;
		}
		if (this.addTypeInfo && headers != null) {
			this.typeMapper.fromJavaType(this.objectMapper.constructType(data.getClass()), headers);
		}
		return serialize(topic, data);
	}

	@Override
	@Nullable
	public byte[] serialize(String topic, @Nullable T data) {
		if (data == null) {
			return null;
		}
		try {
			return this.objectMapper.writeValueAsBytes(data);
		}
		catch (IOException ex) {
			throw new SerializationException("Can't serialize data [" + data + "] for topic [" + topic + "]", ex);
		}
	}

	@Override
	public void close() {
		// No-op
	}

}
