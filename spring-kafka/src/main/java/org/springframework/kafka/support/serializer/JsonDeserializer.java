/*
 * Copyright 2015-2018 the original author or authors.
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
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ExtendedDeserializer;

import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.converter.AbstractJavaTypeMapper;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Generic {@link Deserializer} for receiving JSON from Kafka and return Java objects.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Gary Russell
 * @author Yanming Zhou
 * @author Elliot Kennedy
 */
public class JsonDeserializer<T> implements ExtendedDeserializer<T> {

	/**
	 * Kafka config property for the default key type if no header.
	 * @deprecated in favor of {@link #KEY_DEFAULT_TYPE}
	 */
	@Deprecated
	public static final String DEFAULT_KEY_TYPE = "spring.json.key.default.type";

	/**
	 * Kafka config property for the default value type if no header.
	 * @deprecated in favor of {@link #VALUE_DEFAULT_TYPE}
	 */
	@Deprecated
	public static final String DEFAULT_VALUE_TYPE = "spring.json.default.value.type";

	/**
	 * Kafka config property for the default key type if no header.
	 */
	public static final String KEY_DEFAULT_TYPE = "spring.json.key.default.type";

	/**
	 * Kafka config property for the default value type if no header.
	 */
	public static final String VALUE_DEFAULT_TYPE = "spring.json.value.default.type";

	/**
	 * Kafka config property for trusted deserialization packages.
	 */
	public static final String TRUSTED_PACKAGES = "spring.json.trusted.packages";

	/**
	 * Kafka config property to add type mappings to the type mapper:
	 * 'foo=com.Foo,bar=com.Bar'.
	 */
	public static final String TYPE_MAPPINGS = JsonSerializer.TYPE_MAPPINGS;

	/**
	 * Kafka config property for removing type headers.
	 */
	public static final String REMOVE_TYPE_INFO_HEADERS = "spring.json.remove.type.headers";

	protected final ObjectMapper objectMapper;

	protected Class<T> targetType;

	private volatile ObjectReader reader;

	protected Jackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();

	private boolean typeMapperExplicitlySet = false;

	private boolean removeTypeHeaders = true;

	/**
	 * Construct an instance with a default {@link ObjectMapper}.
	 */
	public JsonDeserializer() {
		this(null, true);
	}

	/**
	 * Construct an instance with the provided {@link ObjectMapper}.
	 * @param objectMapper a custom object mapper.
	 */
	public JsonDeserializer(ObjectMapper objectMapper) {
		this(null, objectMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and a default
	 * {@link ObjectMapper}.
	 * @param targetType the target type to use if no type info headers are present.
	 */
	public JsonDeserializer(Class<T> targetType) {
		this(targetType, true);
	}

	/**
	 * Construct an instance with the provided target type, and
	 * useHeadersIfPresent with a default {@link ObjectMapper}.
	 * @param targetType the target type.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 * @since 2.2
	 */
	public JsonDeserializer(Class<T> targetType, boolean useHeadersIfPresent) {
		this(targetType, new ObjectMapper(), useHeadersIfPresent);
		this.objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Construct an instance with the provided target type, and {@link ObjectMapper}.
	 * @param targetType the target type to use if no type info headers are present.
	 * @param objectMapper the mapper. type if not.
	 */
	public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
		this(targetType, objectMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, {@link ObjectMapper} and
	 * useHeadersIfPresent.
	 * @param targetType the target type.
	 * @param objectMapper the mapper.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 * @since 2.2
	 */
	@SuppressWarnings("unchecked")
	public JsonDeserializer(@Nullable Class<T> targetType, ObjectMapper objectMapper, boolean useHeadersIfPresent) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
		this.targetType = targetType;
		if (this.targetType == null) {
			this.targetType = (Class<T>) ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0);
		}
		Assert.isTrue(this.targetType != null || useHeadersIfPresent,
				"'targetType' cannot be null if 'useHeadersIfPresent' is false");

		if (this.targetType != null) {
			this.reader = this.objectMapper.readerFor(this.targetType);
		}

		addTargetPackageToTrusted();
		this.typeMapper.setTypePrecedence(useHeadersIfPresent ? TypePrecedence.TYPE_ID : TypePrecedence.INFERRED);
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
		if (!this.typeMapperExplicitlySet) {
			if (this.getTypeMapper() instanceof AbstractJavaTypeMapper) {
				AbstractJavaTypeMapper typeMapper = (AbstractJavaTypeMapper) this.getTypeMapper();
				typeMapper.setUseForKey(isKey);
			}
		}
	}

	/**
	 * Set to false to retain type information headers after deserialization.
	 * Default true.
	 * @param removeTypeHeaders true to remove headers.
	 * @since 2.2
	 */
	public void setRemoveTypeHeaders(boolean removeTypeHeaders) {
		this.removeTypeHeaders = removeTypeHeaders;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		setUseTypeMapperForKey(isKey);
		try {
			if (isKey && configs.containsKey(KEY_DEFAULT_TYPE)) {
				if (configs.get(KEY_DEFAULT_TYPE) instanceof Class) {
					this.targetType = (Class<T>) configs.get(KEY_DEFAULT_TYPE);
				}
				else if (configs.get(KEY_DEFAULT_TYPE) instanceof String) {
					this.targetType = (Class<T>) ClassUtils.forName((String) configs.get(KEY_DEFAULT_TYPE), null);
				}
				else {
					throw new IllegalStateException(KEY_DEFAULT_TYPE + " must be Class or String");
				}
			}
			// TODO don't forget to remove these code after DEFAULT_VALUE_TYPE being removed.
			else if (!isKey && configs.containsKey("spring.json.default.value.type")) {
				if (configs.get("spring.json.default.value.type") instanceof Class) {
					this.targetType = (Class<T>) configs.get("spring.json.default.value.type");
				}
				else if (configs.get("spring.json.default.value.type") instanceof String) {
					this.targetType = (Class<T>) ClassUtils
						.forName((String) configs.get("spring.json.default.value.type"), null);
				}
				else {
					throw new IllegalStateException("spring.json.default.value.type must be Class or String");
				}
			}
			else if (!isKey && configs.containsKey(VALUE_DEFAULT_TYPE)) {
				if (configs.get(VALUE_DEFAULT_TYPE) instanceof Class) {
					this.targetType = (Class<T>) configs.get(VALUE_DEFAULT_TYPE);
				}
				else if (configs.get(VALUE_DEFAULT_TYPE) instanceof String) {
					this.targetType = (Class<T>) ClassUtils.forName((String) configs.get(VALUE_DEFAULT_TYPE), null);
				}
				else {
					throw new IllegalStateException(VALUE_DEFAULT_TYPE + " must be Class or String");
				}
			}

			if (this.targetType != null) {
				this.reader = this.objectMapper.readerFor(this.targetType);
			}
			addTargetPackageToTrusted();
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalStateException(e);
		}
		if (configs.containsKey(TRUSTED_PACKAGES)) {
			if (configs.get(TRUSTED_PACKAGES) instanceof String) {
				this.typeMapper.addTrustedPackages(
						StringUtils.commaDelimitedListToStringArray((String) configs.get(TRUSTED_PACKAGES)));
			}
		}
		if (configs.containsKey(TYPE_MAPPINGS) && !this.typeMapperExplicitlySet
				&& this.typeMapper instanceof AbstractJavaTypeMapper) {
			((AbstractJavaTypeMapper) this.typeMapper).setIdClassMapping(
					JsonSerializer.createMappings((String) configs.get(JsonSerializer.TYPE_MAPPINGS)));
		}
		if (configs.containsKey(REMOVE_TYPE_INFO_HEADERS)) {
			this.removeTypeHeaders = Boolean.parseBoolean((String) configs.get(REMOVE_TYPE_INFO_HEADERS));
		}
	}

	/**
	 * Add trusted packages for deserialization.
	 * @param packages the packages.
	 * @since 2.1
	 */
	public void addTrustedPackages(String... packages) {
		this.typeMapper.addTrustedPackages(packages);
	}

	private void addTargetPackageToTrusted() {
		if (this.targetType != null) {
			addTrustedPackages(this.targetType.getPackage().getName());
		}
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		if (data == null) {
			return null;
		}
		ObjectReader reader = null;
		if (this.typeMapper.getTypePrecedence().equals(TypePrecedence.TYPE_ID)) {
			JavaType javaType = this.typeMapper.toJavaType(headers);
			if (javaType != null) {
				reader = this.objectMapper.readerFor(javaType);
			}
		}
		if (this.removeTypeHeaders) {
			this.typeMapper.removeHeaders(headers);
		}
		if (reader == null) {
			reader = this.reader;
		}
		Assert.state(reader != null, "No type information in headers and no default type provided");
		try {
			return reader.readValue(data);
		}
		catch (IOException e) {
			throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) +
					"] from topic [" + topic + "]", e);
		}
	}

	@Override
	public T deserialize(String topic, @Nullable byte[] data) {
		if (data == null) {
			return null;
		}
		Assert.state(this.reader != null, "No headers available and no default type provided");
		try {
			return this.reader.readValue(data);
		}
		catch (IOException e) {
			throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) +
					"] from topic [" + topic + "]", e);
		}
	}

	@Override
	public void close() {
		// No-op
	}

}
