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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.support.JacksonMapperUtils;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Generic {@link org.apache.kafka.common.serialization.Serializer Serializer} for sending
 * Java objects to Kafka as JSON. Based on Jackson 3.
 * <p>
 * IMPORTANT: Configuration must be done completely with property setters or via
 * {@link #configure(Map, boolean)}, not a mixture. If any setters have been called,
 * {@link #configure(Map, boolean)} will be a no-op.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Gary Russell
 * @author Elliot Kennedy
 * @author Wang Zhiyang
 * @author Omer Celik
 * @author Soby Chacko
 * @author Trond Ziarkowski
 *
 * @since 4.0
 */
public class JacksonJsonSerializer<T> implements Serializer<T> {

	/**
	 * Kafka config property for disabling adding type headers.
	 */
	public static final String ADD_TYPE_INFO_HEADERS = "spring.json.add.type.headers";

	/**
	 * Kafka config property to add type mappings to the type mapper:
	 * 'foo:com.Foo,bar:com.Bar'.
	 */
	public static final String TYPE_MAPPINGS = "spring.json.type.mapping";

	protected final JsonMapper jsonMapper; // NOSONAR

	protected boolean addTypeInfo = true; // NOSONAR

	private ObjectWriter writer;

	protected JacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper(); // NOSONAR

	private boolean typeMapperExplicitlySet = false;

	private boolean setterCalled;

	private boolean configured;

	private final Lock globalLock = new ReentrantLock();

	public JacksonJsonSerializer() {
		this((JavaType) null, JacksonMapperUtils.enhancedJsonMapper());
	}

	public JacksonJsonSerializer(TypeReference<? super T> targetType) {
		this(targetType, JacksonMapperUtils.enhancedJsonMapper());
	}

	public JacksonJsonSerializer(JsonMapper jsonMapper) {
		this((JavaType) null, jsonMapper);
	}

	public JacksonJsonSerializer(TypeReference<? super T> targetType, JsonMapper jsonMapper) {
		this(targetType == null ? null : jsonMapper.constructType(targetType.getType()), jsonMapper);
	}

	public JacksonJsonSerializer(@Nullable JavaType targetType, JsonMapper jsonMapper) {
		Assert.notNull(jsonMapper, "'jsonMapper' must not be null.");
		this.jsonMapper = jsonMapper;
		this.writer = jsonMapper.writerFor(targetType);
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
		this.setterCalled = true;
	}

	public JacksonJavaTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Set a customized type mapper.
	 * @param typeMapper the type mapper.
	 * @since 2.1
	 */
	public void setTypeMapper(JacksonJavaTypeMapper typeMapper) {
		Assert.notNull(typeMapper, "'typeMapper' cannot be null");
		this.typeMapper = typeMapper;
		this.typeMapperExplicitlySet = true;
		this.setterCalled = true;
	}

	/**
	 * Configure the default Jackson2JavaTypeMapper to use key type headers.
	 * @param isKey Use key type headers if true
	 * @since 2.1.3
	 */
	public void setUseTypeMapperForKey(boolean isKey) {
		if (!this.typeMapperExplicitlySet && getTypeMapper() instanceof DefaultJacksonJavaTypeMapper) {
			((DefaultJacksonJavaTypeMapper) getTypeMapper())
					.setUseForKey(isKey);
		}
		this.setterCalled = true;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		try {
			this.globalLock.lock();
			if (this.configured) {
				return;
			}
			Assert.state(!this.setterCalled
							|| (!configs.containsKey(ADD_TYPE_INFO_HEADERS) && !configs.containsKey(TYPE_MAPPINGS)),
					"JsonSerializer must be configured with property setters, or via configuration properties; not both");
			setUseTypeMapperForKey(isKey);
			if (configs.containsKey(ADD_TYPE_INFO_HEADERS)) {
				Object config = configs.get(ADD_TYPE_INFO_HEADERS);
				if (config instanceof Boolean configBoolean) {
					this.addTypeInfo = configBoolean;
				}
				else if (config instanceof String configString) {
					this.addTypeInfo = Boolean.parseBoolean(configString);
				}
				else {
					throw new IllegalStateException(ADD_TYPE_INFO_HEADERS + " must be Boolean or String");
				}
			}
			if (configs.containsKey(TYPE_MAPPINGS) && !this.typeMapperExplicitlySet
					&& this.typeMapper instanceof DefaultJacksonJavaTypeMapper abstractJavaTypeMapper) {
				abstractJavaTypeMapper.setIdClassMapping(createMappings((String) configs.get(TYPE_MAPPINGS)));
			}
			this.configured = true;
		}
		finally {
			this.globalLock.unlock();
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
				throw new IllegalArgumentException("Failed to load: " + split[1] + " for " + split[0], e);
			}
		}
		return mappingsMap;
	}

	@Override
	public byte @Nullable [] serialize(String topic, Headers headers, @Nullable T data) {
		if (data == null) {
			return null;
		}
		if (this.addTypeInfo && headers != null) {
			this.typeMapper.fromJavaType(this.jsonMapper.constructType(data.getClass()), headers);
		}
		return serialize(topic, data);
	}

	@Override
	public byte @Nullable [] serialize(String topic, @Nullable T data) {
		if (data == null) {
			return null;
		}
		try {
			return this.writer.writeValueAsBytes(data);
		}
		catch (Exception ex) {
			throw new SerializationException("Can't serialize data [" + data + "] for topic [" + topic + "]", ex);
		}
	}

	@Override
	public void close() {
		// No-op
	}

	/**
	 * Copies this serializer with same configuration, except new target type reference is used.
	 * @param newTargetType type reference forced for serialization, not null
	 * @param <X> new serialization source type
	 * @return new instance of serializer with type changes
	 * @since 2.6
	 */
	public <X> JacksonJsonSerializer<X> copyWithType(Class<? super X> newTargetType) {
		return copyWithType(this.jsonMapper.constructType(newTargetType));
	}

	/**
	 * Copies this serializer with same configuration, except new target type reference is used.
	 * @param newTargetType type reference forced for serialization, not null
	 * @param <X> new serialization source type
	 * @return new instance of serializer with type changes
	 * @since 2.6
	 */
	public <X> JacksonJsonSerializer<X> copyWithType(TypeReference<? super X> newTargetType) {
		return copyWithType(this.jsonMapper.constructType(newTargetType.getType()));
	}

	/**
	 * Copies this serializer with same configuration, except new target java type is used.
	 * @param newTargetType java type forced for serialization, not null
	 * @param <X> new serialization source type
	 * @return new instance of serializer with type changes
	 * @since 2.6
	 */
	public <X> JacksonJsonSerializer<X> copyWithType(JavaType newTargetType) {
		JacksonJsonSerializer<X> result = new JacksonJsonSerializer<>(newTargetType, this.jsonMapper);
		result.addTypeInfo = this.addTypeInfo;
		result.typeMapper = this.typeMapper;
		result.typeMapperExplicitlySet = this.typeMapperExplicitlySet;
		return result;
	}

	// Fluent API

	/**
	 * Designate this serializer for serializing keys (default is values); only applies if
	 * the default type mapper is used.
	 * @return the serializer.
	 * @since 2.3
	 * @see #setUseTypeMapperForKey(boolean)
	 */
	public JacksonJsonSerializer<T> forKeys() {
		setUseTypeMapperForKey(true);
		return this;
	}

	/**
	 * Do not include type info headers.
	 * @return the serializer.
	 * @since 2.3
	 * @see #setAddTypeInfo(boolean)
	 */
	public JacksonJsonSerializer<T> noTypeInfo() {
		setAddTypeInfo(false);
		return this;
	}

	/**
	 * Use the supplied {@link JacksonJavaTypeMapper}.
	 * @param mapper the mapper.
	 * @return the serializer.
	 * @since 2.3
	 * @see #setTypeMapper(JacksonJavaTypeMapper)
	 */
	public JacksonJsonSerializer<T> typeMapper(JacksonJavaTypeMapper mapper) {
		setTypeMapper(mapper);
		return this;
	}

}
