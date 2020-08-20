/*
 * Copyright 2015-2020 the original author or authors.
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.converter.AbstractJavaTypeMapper;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Generic {@link org.apache.kafka.common.serialization.Deserializer Deserializer} for
 * receiving JSON from Kafka and return Java objects.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Gary Russell
 * @author Yanming Zhou
 * @author Elliot Kennedy
 * @author Torsten Schleede
 * @author Ivan Ponomarev
 */
public class JsonDeserializer<T> implements Deserializer<T> {

	private static final String KEY_DEFAULT_TYPE_STRING = "spring.json.key.default.type";

	/**
	 * Kafka config property for the default key type if no header.
	 */
	public static final String KEY_DEFAULT_TYPE = KEY_DEFAULT_TYPE_STRING;

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
	 * Kafka config property for removing type headers (default true).
	 */
	public static final String REMOVE_TYPE_INFO_HEADERS = "spring.json.remove.type.headers";

	/**
	 * Kafka config property for using type headers (default true).
	 * @since 2.2.3
	 */
	public static final String USE_TYPE_INFO_HEADERS = "spring.json.use.type.headers";

	/**
	 * A method name to determine the {@link JavaType} to deserialize the key to.
	 */
	public static final String KEY_TYPE_METHOD = "spring.json.key.type.method";

	/**
	 * A method name to determine the {@link JavaType} to deserialize the key to.
	 */
	public static final String VALUE_TYPE_METHOD = "spring.json.value.type.method";

	protected final ObjectMapper objectMapper; // NOSONAR

	protected JavaType targetType; // NOSONAR

	protected Jackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper(); // NOSONAR

	private ObjectReader reader;

	private boolean typeMapperExplicitlySet = false;

	private boolean removeTypeHeaders = true;

	private boolean useTypeHeaders = true;

	private JsonTypeResolver typeResolver;

	/**
	 * Construct an instance with a default {@link ObjectMapper}.
	 */
	public JsonDeserializer() {
		this((Class<T>) null, true);
	}

	/**
	 * Construct an instance with the provided {@link ObjectMapper}.
	 * @param objectMapper a custom object mapper.
	 */
	public JsonDeserializer(ObjectMapper objectMapper) {
		this((Class<T>) null, objectMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and a default
	 * {@link ObjectMapper}.
	 * @param targetType the target type to use if no type info headers are present.
	 */
	public JsonDeserializer(@Nullable Class<? super T> targetType) {
		this(targetType, true);
	}

	/**
	 * Construct an instance with the provided target type, and a default {@link ObjectMapper}.
	 * @param targetType the target type reference to use if no type info headers are present.
	 * @since 2.3
	 */
	public JsonDeserializer(@Nullable TypeReference<? super T> targetType) {
		this(targetType, true);
	}


	/**
	 * Construct an instance with the provided target type, and a default {@link ObjectMapper}.
	 * @param targetType the target java type to use if no type info headers are present.
	 * @since 2.3
	 */
	public JsonDeserializer(@Nullable JavaType targetType) {
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
	public JsonDeserializer(@Nullable Class<? super T> targetType, boolean useHeadersIfPresent) {
		this(targetType, JacksonUtils.enhancedObjectMapper(), useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, and
	 * useHeadersIfPresent with a default {@link ObjectMapper}.
	 * @param targetType the target type reference.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 * @since 2.3
	 */
	public JsonDeserializer(TypeReference<? super T> targetType, boolean useHeadersIfPresent) {
		this(targetType, JacksonUtils.enhancedObjectMapper(), useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, and
	 * useHeadersIfPresent with a default {@link ObjectMapper}.
	 * @param targetType the target java type.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 * @since 2.3
	 */
	public JsonDeserializer(JavaType targetType, boolean useHeadersIfPresent) {
		this(targetType, JacksonUtils.enhancedObjectMapper(), useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, and {@link ObjectMapper}.
	 * @param targetType the target type to use if no type info headers are present.
	 * @param objectMapper the mapper. type if not.
	 */
	public JsonDeserializer(Class<? super T> targetType, ObjectMapper objectMapper) {
		this(targetType, objectMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and {@link ObjectMapper}.
	 * @param targetType the target type reference to use if no type info headers are present.
	 * @param objectMapper the mapper. type if not.
	 */
	public JsonDeserializer(TypeReference<? super T> targetType, ObjectMapper objectMapper) {
		this(targetType, objectMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and {@link ObjectMapper}.
	 * @param targetType the target java type to use if no type info headers are present.
	 * @param objectMapper the mapper. type if not.
	 */
	public JsonDeserializer(JavaType targetType, ObjectMapper objectMapper) {
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
	public JsonDeserializer(@Nullable Class<? super T> targetType, ObjectMapper objectMapper,
			boolean useHeadersIfPresent) {

		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
		JavaType javaType = null;
		if (targetType == null) {
			Class<?> genericType = ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0);
			if (genericType != null) {
				javaType = TypeFactory.defaultInstance().constructType(genericType);
			}
		}
		else {
			javaType = TypeFactory.defaultInstance().constructType(targetType);
		}

		initialize(javaType, useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, {@link ObjectMapper} and
	 * useHeadersIfPresent.
	 * @param targetType the target type reference.
	 * @param objectMapper the mapper.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 * @since 2.3
	 */
	public JsonDeserializer(TypeReference<? super T> targetType, ObjectMapper objectMapper,
			boolean useHeadersIfPresent) {

		this(targetType != null ? TypeFactory.defaultInstance().constructType(targetType) : null,
				objectMapper, useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, {@link ObjectMapper} and
	 * useHeadersIfPresent.
	 * @param targetType the target type reference.
	 * @param objectMapper the mapper.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 * @since 2.3
	 */
	public JsonDeserializer(@Nullable JavaType targetType, ObjectMapper objectMapper,
			boolean useHeadersIfPresent) {

		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
		initialize(targetType, useHeadersIfPresent);
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
		if (!this.typeMapperExplicitlySet
				&& this.getTypeMapper() instanceof AbstractJavaTypeMapper) {
			((AbstractJavaTypeMapper) this.getTypeMapper()).setUseForKey(isKey);
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

	/**
	 * Set to false to ignore type information in headers and use the configured
	 * target type instead.
	 * Only applies if the preconfigured type mapper is used.
	 * Default true.
	 * @param useTypeHeaders false to ignore type headers.
	 * @since 2.2.8
	 */
	public void setUseTypeHeaders(boolean useTypeHeaders) {
		if (!this.typeMapperExplicitlySet) {
			this.useTypeHeaders = useTypeHeaders;
			setUpTypePrecedence(Collections.emptyMap());
		}
	}

	/**
	 * Set a {@link BiFunction} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param typeFunction the function.
	 * @since 2.5
	 */
	public void setTypeFunction(BiFunction<byte[], Headers, JavaType> typeFunction) {
		this.typeResolver = (topic, data, headers) -> typeFunction.apply(data, headers);
	}

	/**
	 * Set a {@link JsonTypeResolver} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param typeResolver the resolver.
	 * @since 2.5.3
	 */
	public void setTypeResolver(JsonTypeResolver typeResolver) {
		this.typeResolver = typeResolver;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		setUseTypeMapperForKey(isKey);
		setUpTypePrecedence(configs);
		setupTarget(configs, isKey);
		if (configs.containsKey(TRUSTED_PACKAGES)
				&& configs.get(TRUSTED_PACKAGES) instanceof String) {
			this.typeMapper.addTrustedPackages(
					StringUtils.delimitedListToStringArray((String) configs.get(TRUSTED_PACKAGES), ",", " \r\n\f\t"));
		}
		if (configs.containsKey(TYPE_MAPPINGS) && !this.typeMapperExplicitlySet
				&& this.typeMapper instanceof AbstractJavaTypeMapper) {
			((AbstractJavaTypeMapper) this.typeMapper).setIdClassMapping(
					JsonSerializer.createMappings(configs.get(JsonSerializer.TYPE_MAPPINGS).toString()));
		}
		if (configs.containsKey(REMOVE_TYPE_INFO_HEADERS)) {
			this.removeTypeHeaders = Boolean.parseBoolean(configs.get(REMOVE_TYPE_INFO_HEADERS).toString());
		}
		setUpTypeMethod(configs, isKey);
	}

	private void setUpTypeMethod(Map<String, ?> configs, boolean isKey) {
		if (isKey && configs.containsKey(KEY_TYPE_METHOD)) {
			setUpTypeResolver((String) configs.get(KEY_TYPE_METHOD));
		}
		else if (!isKey && configs.containsKey(VALUE_TYPE_METHOD)) {
			setUpTypeResolver((String) configs.get(VALUE_TYPE_METHOD));
		}
	}

	private void setUpTypeResolver(String method) {
		try {
			this.typeResolver = buildTypeResolver(method);
			return;
		}
		catch (IllegalStateException e) {
			if (e.getCause() instanceof NoSuchMethodException) {
				this.typeResolver = (topic, data, headers) ->
					(JavaType) SerializationUtils.propertyToMethodInvokingFunction(
							method, byte[].class, getClass().getClassLoader()).apply(data, headers);
				return;
			}
			throw e;
		}
	}

	private void setUpTypePrecedence(Map<String, ?> configs) {
		if (!this.typeMapperExplicitlySet) {
			if (configs.containsKey(USE_TYPE_INFO_HEADERS)) {
				this.useTypeHeaders = Boolean.parseBoolean(configs.get(USE_TYPE_INFO_HEADERS).toString());
			}
			this.typeMapper.setTypePrecedence(this.useTypeHeaders ? TypePrecedence.TYPE_ID : TypePrecedence.INFERRED);
		}
	}

	private void setupTarget(Map<String, ?> configs, boolean isKey) {
		try {
			JavaType javaType = null;
			if (isKey && configs.containsKey(KEY_DEFAULT_TYPE)) {
				javaType = setupTargetType(configs, KEY_DEFAULT_TYPE);
			}
			else if (!isKey && configs.containsKey(VALUE_DEFAULT_TYPE)) {
				javaType = setupTargetType(configs, VALUE_DEFAULT_TYPE);
			}

			if (javaType != null) {
				initialize(javaType, TypePrecedence.TYPE_ID.equals(this.typeMapper.getTypePrecedence()));
			}
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalStateException(e);
		}
	}

	private void initialize(@Nullable JavaType type, boolean useHeadersIfPresent) {
		this.targetType = type;
		Assert.isTrue(this.targetType != null || useHeadersIfPresent,
				"'targetType' cannot be null if 'useHeadersIfPresent' is false");

		if (this.targetType != null) {
			this.reader = this.objectMapper.readerFor(this.targetType);
		}

		addTargetPackageToTrusted();
		this.typeMapper.setTypePrecedence(useHeadersIfPresent ? TypePrecedence.TYPE_ID : TypePrecedence.INFERRED);
	}

	private JavaType setupTargetType(Map<String, ?> configs, String key) throws ClassNotFoundException, LinkageError {
		if (configs.get(key) instanceof Class) {
			return TypeFactory.defaultInstance().constructType((Class<?>) configs.get(key));
		}
		else if (configs.get(key) instanceof String) {
			return TypeFactory.defaultInstance()
							.constructType(ClassUtils.forName((String) configs.get(key), null));
		}
		else {
			throw new IllegalStateException(key + " must be Class or String");
		}
	}

	/**
	 * Add trusted packages for deserialization.
	 * @param packages the packages.
	 * @since 2.1
	 */
	public void addTrustedPackages(String... packages) {
		doAddTrustedPackages(packages);
	}

	private void addTargetPackageToTrusted() {
		String targetPackageName = getTargetPackageName();
		if (targetPackageName != null) {
			doAddTrustedPackages(targetPackageName);
			doAddTrustedPackages(targetPackageName + ".*");
		}
	}

	private String getTargetPackageName() {
		if (this.targetType != null) {
			return ClassUtils.getPackageName(this.targetType.getRawClass()).replaceFirst("\\[L", "");
		}
		return null;
	}

	private void doAddTrustedPackages(String... packages) {
		this.typeMapper.addTrustedPackages(packages);
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		if (data == null) {
			return null;
		}
		ObjectReader deserReader = null;
		JavaType javaType = null;
		if (this.typeResolver != null) {
			javaType = this.typeResolver.resolveType(topic, data, headers);
		}
		if (javaType == null && this.typeMapper.getTypePrecedence().equals(TypePrecedence.TYPE_ID)) {
			javaType = this.typeMapper.toJavaType(headers);
		}
		if (javaType != null) {
			deserReader = this.objectMapper.readerFor(javaType);
		}
		if (this.removeTypeHeaders) {
			this.typeMapper.removeHeaders(headers);
		}
		if (deserReader == null) {
			deserReader = this.reader;
		}
		Assert.state(deserReader != null, "No type information in headers and no default type provided");
		try {
			return deserReader.readValue(data);
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
		ObjectReader localReader = this.reader;
		if (this.typeResolver != null) {
			JavaType javaType = this.typeResolver.resolveType(topic, data, null);
			if (javaType != null) {
				localReader = this.objectMapper.readerFor(javaType);
			}
		}
		Assert.state(localReader != null, "No headers available and no default type provided");
		try {
			return localReader.readValue(data);
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

	/**
	 * Copies this deserializer with same configuration, except new target type is used.
	 * @param newTargetType type used for when type headers are missing, not null
	 * @param <X> new deserialization result type
	 * @return new instance of deserializer with type changes
	 * @since 2.6
	 */
	public <X> JsonDeserializer<X> copyWithType(Class<? super X> newTargetType) {
		return copyWithType(this.objectMapper.constructType(newTargetType));
	}

	/**
	 * Copies this deserializer with same configuration, except new target type reference is used.
	 * @param newTargetType type reference used for when type headers are missing, not null
	 * @param <X> new deserialization result type
	 * @return new instance of deserializer with type changes
	 * @since 2.6
	 */
	public <X> JsonDeserializer<X> copyWithType(TypeReference<? super X> newTargetType) {
		return copyWithType(this.objectMapper.constructType(newTargetType.getType()));
	}

	/**
	 * Copies this deserializer with same configuration, except new target java type is used.
	 * @param newTargetType java type used for when type headers are missing, not null
	 * @param <X> new deserialization result type
	 * @return new instance of deserializer with type changes
	 * @since 2.6
	 */
	public <X> JsonDeserializer<X> copyWithType(JavaType newTargetType) {
		JsonDeserializer<X> result = new JsonDeserializer<>(newTargetType, this.objectMapper, this.useTypeHeaders);
		result.removeTypeHeaders = this.removeTypeHeaders;
		result.typeMapper = this.typeMapper;
		result.typeMapperExplicitlySet = this.typeMapperExplicitlySet;
		return result;
	}

	// Fluent API

	/**
	 * Designate this deserializer for deserializing keys (default is values); only
	 * applies if the default type mapper is used.
	 * @return the deserializer.
	 * @since 2.3
	 */
	public JsonDeserializer<T> forKeys() {
		setUseTypeMapperForKey(true);
		return this;
	}

	/**
	 * Don't remove type information headers.
	 * @return the deserializer.
	 * @since 2.3
	 * @see #setRemoveTypeHeaders(boolean)
	 */
	public JsonDeserializer<T> dontRemoveTypeHeaders() {
		setRemoveTypeHeaders(false);
		return this;
	}

	/**
	 * Ignore type information headers and use the configured target class.
	 * @return the deserializer.
	 * @since 2.3
	 * @see #setUseTypeHeaders(boolean)
	 */
	public JsonDeserializer<T> ignoreTypeHeaders() {
		setUseTypeHeaders(false);
		return this;
	}

	/**
	 * Use the supplied {@link Jackson2JavaTypeMapper}.
	 * @param mapper the mapper.
	 * @return the deserializer.
	 * @since 2.3
	 * @see #setTypeMapper(Jackson2JavaTypeMapper)
	 */
	public JsonDeserializer<T> typeMapper(Jackson2JavaTypeMapper mapper) {
		setTypeMapper(mapper);
		return this;
	}

	/**
	 * Add trusted packages to the default type mapper.
	 * @param packages the packages.
	 * @return the deserializer.
	 * @since 2,5
	 */
	public JsonDeserializer<T> trustedPackages(String... packages) {
		Assert.isTrue(!this.typeMapperExplicitlySet, "When using a custom type mapper, set the trusted packages there");
		this.typeMapper.addTrustedPackages(packages);
		return this;
	}

	/**
	 * Set a {@link BiFunction} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param typeFunction the function.
	 * @return the deserializer.
	 * @since 2.5
	 */
	public JsonDeserializer<T> typeFunction(BiFunction<byte[], Headers, JavaType> typeFunction) {
		setTypeFunction(typeFunction);
		return this;
	}

	/**
	 * Set a {@link JsonTypeResolver} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param resolver the resolver.
	 * @return the deserializer.
	 * @since 2.5.3
	 */
	public JsonDeserializer<T> typeResolver(JsonTypeResolver resolver) {
		setTypeResolver(resolver);
		return this;
	}

	private JsonTypeResolver buildTypeResolver(String methodProperty) {
		int lastDotPosn = methodProperty.lastIndexOf('.');
		Assert.state(lastDotPosn > 1,
				"the method property needs to be a class name followed by the method name, separated by '.'");
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(methodProperty.substring(0, lastDotPosn), getClass().getClassLoader());
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalStateException(e);
		}
		String methodName = methodProperty.substring(lastDotPosn + 1);
		Method method;
		try {
			method = clazz.getDeclaredMethod(methodName, String.class, byte[].class, Headers.class);
			Assert.state(JavaType.class.isAssignableFrom(method.getReturnType()),
					method + " return type must be JavaType");
			Assert.state(Modifier.isStatic(method.getModifiers()), method + " must be static");
		}
		catch (SecurityException | NoSuchMethodException e) {
			throw new IllegalStateException(e);
		}
		return (topic, data, headers) -> {
			try {
				return (JavaType) method.invoke(null, topic, data, headers);
			}
			catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new IllegalStateException(e);
			}
		};
	}

}
