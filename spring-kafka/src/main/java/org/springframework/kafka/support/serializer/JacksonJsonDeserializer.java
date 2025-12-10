/*
 * Copyright 2015-present the original author or authors.
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
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.TypeFactory;

import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.JacksonMapperUtils;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Generic {@link org.apache.kafka.common.serialization.Deserializer Deserializer} for
 * receiving JSON from Kafka and return Java objects. Based on Jackson 3.
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
 * @author Yanming Zhou
 * @author Elliot Kennedy
 * @author Torsten Schleede
 * @author Ivan Ponomarev
 * @author Omer Celik
 * @author Soby Chacko
 * @author Trond Ziarkowski
 *
 * @since 4.0
 */
@SuppressWarnings("removal")
public class JacksonJsonDeserializer<T> implements Deserializer<T> {

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
	 * Kafka config property for removing type headers (default true).
	 */
	public static final String REMOVE_TYPE_INFO_HEADERS = "spring.json.remove.type.headers";

	/**
	 * Kafka config property for using type headers (default true).
	 * @since 2.2.3
	 */
	public static final String USE_TYPE_INFO_HEADERS = "spring.json.use.type.headers";

	/**
	 * A method name to determine the {@link JavaType} to deserialize the key to:
	 * 'com.Foo.deserialize'. See {@link JacksonJsonTypeResolver#resolveType} for the signature.
	 */
	public static final String KEY_TYPE_METHOD = "spring.json.key.type.method";

	/**
	 * A method name to determine the {@link JavaType} to deserialize the value to:
	 * 'com.Foo.deserialize'. See {@link JacksonJsonTypeResolver#resolveType} for the signature.
	 */
	public static final String VALUE_TYPE_METHOD = "spring.json.value.type.method";

	private static final Set<String> OUR_KEYS = new HashSet<>();

	static {
		OUR_KEYS.add(KEY_DEFAULT_TYPE);
		OUR_KEYS.add(VALUE_DEFAULT_TYPE);
		OUR_KEYS.add(TRUSTED_PACKAGES);
		OUR_KEYS.add(TYPE_MAPPINGS);
		OUR_KEYS.add(REMOVE_TYPE_INFO_HEADERS);
		OUR_KEYS.add(USE_TYPE_INFO_HEADERS);
		OUR_KEYS.add(KEY_TYPE_METHOD);
		OUR_KEYS.add(VALUE_TYPE_METHOD);
	}

	protected final JsonMapper jsonMapper; // NOSONAR

	protected @Nullable JavaType targetType; // NOSONAR

	protected JacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper(); // NOSONAR

	private @Nullable ObjectReader reader;

	private boolean typeMapperExplicitlySet = false;

	private boolean removeTypeHeaders = true;

	private boolean useTypeHeaders = true;

	private @Nullable JacksonJsonTypeResolver typeResolver;

	private boolean setterCalled;

	private boolean configured;

	private final Lock trustedPackagesLock = new ReentrantLock();

	private final TypeFactory typeFactory = TypeFactory.createDefaultInstance();

	/**
	 * Construct an instance with a default {@link JsonMapper}.
	 */
	public JacksonJsonDeserializer() {
		this((Class<T>) null, true);
	}

	/**
	 * Construct an instance with the provided {@link JsonMapper}.
	 * @param jsonMapper a custom object mapper.
	 */
	public JacksonJsonDeserializer(JsonMapper jsonMapper) {
		this((Class<T>) null, jsonMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and a default
	 * {@link JsonMapper}.
	 * @param targetType the target type to use if no type info headers are present.
	 */
	public JacksonJsonDeserializer(@Nullable Class<? super T> targetType) {
		this(targetType, true);
	}

	/**
	 * Construct an instance with the provided target type, and a default {@link JsonMapper}.
	 * @param targetType the target type reference to use if no type info headers are present.
	 */
	public JacksonJsonDeserializer(@Nullable TypeReference<? super T> targetType) {
		this(targetType, true);
	}

	/**
	 * Construct an instance with the provided target type, and a default {@link JsonMapper}.
	 * @param targetType the target java type to use if no type info headers are present.
	 */
	public JacksonJsonDeserializer(@Nullable JavaType targetType) {
		this(targetType, true);
	}

	/**
	 * Construct an instance with the provided target type, and
	 * useHeadersIfPresent with a default {@link JsonMapper}.
	 * @param targetType the target type.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 */
	public JacksonJsonDeserializer(@Nullable Class<? super T> targetType, boolean useHeadersIfPresent) {
		this(targetType, JacksonMapperUtils.enhancedJsonMapper(), useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, and
	 * useHeadersIfPresent with a default {@link JsonMapper}.
	 * @param targetType the target type reference.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 */
	public JacksonJsonDeserializer(@Nullable TypeReference<? super T> targetType, boolean useHeadersIfPresent) {
		this(targetType, JacksonMapperUtils.enhancedJsonMapper(), useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, and
	 * useHeadersIfPresent with a default {@link JsonMapper}.
	 * @param targetType the target java type.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 */
	public JacksonJsonDeserializer(@Nullable JavaType targetType, boolean useHeadersIfPresent) {
		this(targetType, JacksonMapperUtils.enhancedJsonMapper(), useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, and {@link JsonMapper}.
	 * @param targetType the target type to use if no type info headers are present.
	 * @param jsonMapper the mapper. type if not.
	 */
	public JacksonJsonDeserializer(Class<? super T> targetType, JsonMapper jsonMapper) {
		this(targetType, jsonMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and {@link JsonMapper}.
	 * @param targetType the target type reference to use if no type info headers are present.
	 * @param jsonMapper the mapper. type if not.
	 */
	public JacksonJsonDeserializer(TypeReference<? super T> targetType, JsonMapper jsonMapper) {
		this(targetType, jsonMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, and {@link JsonMapper}.
	 * @param targetType the target java type to use if no type info headers are present.
	 * @param jsonMapper the mapper. type if not.
	 */
	public JacksonJsonDeserializer(@Nullable JavaType targetType, JsonMapper jsonMapper) {
		this(targetType, jsonMapper, true);
	}

	/**
	 * Construct an instance with the provided target type, {@link JsonMapper} and
	 * useHeadersIfPresent.
	 * @param targetType the target type.
	 * @param jsonMapper the mapper.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 */
	public JacksonJsonDeserializer(@Nullable Class<? super T> targetType, JsonMapper jsonMapper,
			boolean useHeadersIfPresent) {

		Assert.notNull(jsonMapper, "'jsonMapper' must not be null.");
		this.jsonMapper = jsonMapper;
		JavaType javaType = null;
		if (targetType == null) {
			Class<?> genericType = ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0);
			if (genericType != null) {
				javaType = this.typeFactory.constructType(genericType);
			}
		}
		else {
			javaType = this.typeFactory.constructType(targetType);
		}

		initialize(javaType, useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, {@link JsonMapper} and
	 * useHeadersIfPresent.
	 * @param targetType the target type reference.
	 * @param jsonMapper the mapper.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 */
	public JacksonJsonDeserializer(@Nullable TypeReference<? super T> targetType, JsonMapper jsonMapper,
			boolean useHeadersIfPresent) {

		this(targetType != null ? TypeFactory.createDefaultInstance().constructType(targetType) : null,
				jsonMapper, useHeadersIfPresent);
	}

	/**
	 * Construct an instance with the provided target type, {@link JsonMapper} and
	 * useHeadersIfPresent.
	 * @param targetType the target type reference.
	 * @param jsonMapper the mapper.
	 * @param useHeadersIfPresent true to use headers if present and fall back to target
	 * type if not.
	 */
	public JacksonJsonDeserializer(@Nullable JavaType targetType, JsonMapper jsonMapper,
			boolean useHeadersIfPresent) {

		Assert.notNull(jsonMapper, "'jsonMapper' must not be null.");
		this.jsonMapper = jsonMapper;
		initialize(targetType, useHeadersIfPresent);
	}

	public JacksonJavaTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Set a customized type mapper. If the mapper is a {@link JacksonJavaTypeMapper},
	 * any class mappings configured in the mapper will be added to the trusted packages.
	 * @param typeMapper the type mapper.
	 */
	public void setTypeMapper(JacksonJavaTypeMapper typeMapper) {
		Assert.notNull(typeMapper, "'typeMapper' cannot be null");
		this.typeMapper = typeMapper;
		this.typeMapperExplicitlySet = true;
		if (typeMapper instanceof DefaultJacksonJavaTypeMapper) {
			addMappingsToTrusted(((DefaultJacksonJavaTypeMapper) typeMapper).getIdClassMapping());
		}
		this.setterCalled = true;
	}

	/**
	 * Configure the default JacksonJavaTypeMapper to use key type headers.
	 * @param isKey Use key type headers if true
	 */
	public void setUseTypeMapperForKey(boolean isKey) {
		doSetUseTypeMapperForKey(isKey);
		this.setterCalled = true;
	}

	private void doSetUseTypeMapperForKey(boolean isKey) {
		if (!this.typeMapperExplicitlySet
				&& this.getTypeMapper() instanceof DefaultJacksonJavaTypeMapper) {
			((DefaultJacksonJavaTypeMapper) this.getTypeMapper()).setUseForKey(isKey);
		}
	}

	/**
	 * Set to false to retain type information headers after deserialization.
	 * Default true.
	 * @param removeTypeHeaders true to remove headers.
	 */
	public void setRemoveTypeHeaders(boolean removeTypeHeaders) {
		this.removeTypeHeaders = removeTypeHeaders;
		this.setterCalled = true;
	}

	/**
	 * Set to false to ignore type information in headers and use the configured
	 * target type instead.
	 * Only applies if the preconfigured type mapper is used.
	 * Default true.
	 * @param useTypeHeaders false to ignore type headers.
	 */
	public void setUseTypeHeaders(boolean useTypeHeaders) {
		if (!this.typeMapperExplicitlySet) {
			this.useTypeHeaders = useTypeHeaders;
			setUpTypePrecedence(Collections.emptyMap());
		}
		this.setterCalled = true;
	}

	/**
	 * Set a {@link BiFunction} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param typeFunction the function.
	 */
	public void setTypeFunction(BiFunction<byte[], Headers, JavaType> typeFunction) {
		this.typeResolver = (topic, data, headers) -> typeFunction.apply(data, headers);
		this.setterCalled = true;
	}

	/**
	 * Set a {@link JacksonJsonTypeResolver} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param typeResolver the resolver.
	 */
	public void setTypeResolver(JacksonJsonTypeResolver typeResolver) {
		this.typeResolver = typeResolver;
		this.setterCalled = true;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		try {
			this.trustedPackagesLock.lock();
			if (this.configured) {
				return;
			}
			Assert.state(!this.setterCalled || !configsHasOurKeys(configs),
					"JsonDeserializer must be configured with property setters, or via configuration properties; not both");
			doSetUseTypeMapperForKey(isKey);
			setUpTypePrecedence(configs);
			setupTarget(configs, isKey);
			if (configs.containsKey(TRUSTED_PACKAGES)
					&& configs.get(TRUSTED_PACKAGES) instanceof String) {
				this.typeMapper.addTrustedPackages(
						StringUtils.delimitedListToStringArray((String) configs.get(TRUSTED_PACKAGES), ",", " \r\n\f\t"));
			}
			if (configs.containsKey(TYPE_MAPPINGS) && !this.typeMapperExplicitlySet
					&& this.typeMapper instanceof DefaultJacksonJavaTypeMapper) {
				((DefaultJacksonJavaTypeMapper) this.typeMapper).setIdClassMapping(createMappings(configs));
			}
			if (configs.containsKey(REMOVE_TYPE_INFO_HEADERS)) {
				this.removeTypeHeaders = Boolean.parseBoolean(configs.get(REMOVE_TYPE_INFO_HEADERS).toString());
			}
			setUpTypeMethod(configs, isKey);
			this.configured = true;
		}
		finally {
			this.trustedPackagesLock.unlock();
		}
	}

	private boolean configsHasOurKeys(Map<String, ?> configs) {
		for (String key : configs.keySet()) {
			if (OUR_KEYS.contains(key)) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private Map<String, Class<?>> createMappings(Map<String, ?> configs) {
		Map<String, Class<?>> mappings =
				JacksonJsonSerializer.createMappings(configs.get(JacksonJsonSerializer.TYPE_MAPPINGS).toString());
		addMappingsToTrusted(mappings);
		return mappings;
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
			this.typeMapper.setTypePrecedence(this.useTypeHeaders ? JacksonJavaTypeMapper.TypePrecedence.TYPE_ID
					: JacksonJavaTypeMapper.TypePrecedence.INFERRED);
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
				initialize(javaType, JacksonJavaTypeMapper.TypePrecedence.TYPE_ID.equals(
						this.typeMapper.getTypePrecedence()));
			}
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalStateException(e);
		}
	}

	private void initialize(@Nullable JavaType type, boolean useHeadersIfPresent) {
		this.targetType = type;
		this.useTypeHeaders = useHeadersIfPresent;
		Assert.isTrue(this.targetType != null || useHeadersIfPresent,
				"'targetType' cannot be null if 'useHeadersIfPresent' is false");

		if (this.targetType != null) {
			this.reader = this.jsonMapper.readerFor(this.targetType);
		}

		addTargetPackageToTrusted();
		this.typeMapper.setTypePrecedence(useHeadersIfPresent ? JacksonJavaTypeMapper.TypePrecedence.TYPE_ID
				: JacksonJavaTypeMapper.TypePrecedence.INFERRED);
	}

	private JavaType setupTargetType(Map<String, ?> configs, String key) throws ClassNotFoundException, LinkageError {
		if (configs.get(key) instanceof Class) {
			return this.typeFactory.constructType((Class<?>) configs.get(key));
		}
		else if (configs.get(key) instanceof String) {
			return this.typeFactory
					.constructType(ClassUtils.forName((String) configs.get(key), null));
		}
		else {
			throw new IllegalStateException(key + " must be Class or String");
		}
	}

	/**
	 * Add trusted packages for deserialization.
	 * @param packages the packages.
	 */
	public void addTrustedPackages(String... packages) {
		try {
			this.trustedPackagesLock.lock();
			doAddTrustedPackages(packages);
			this.setterCalled = true;
		}
		finally {
			this.trustedPackagesLock.unlock();
		}
	}

	private void addMappingsToTrusted(Map<String, Class<?>> mappings) {
		mappings.values().forEach(clazz -> {
			String packageName = clazz.isArray()
					? clazz.getComponentType().getPackage().getName()
					: clazz.getPackage().getName();
			doAddTrustedPackages(packageName);
			doAddTrustedPackages(packageName + ".*");
		});
	}

	private void addTargetPackageToTrusted() {
		String targetPackageName = getTargetPackageName();
		if (targetPackageName != null) {
			doAddTrustedPackages(targetPackageName);
			doAddTrustedPackages(targetPackageName + ".*");
		}
	}

	private @Nullable String getTargetPackageName() {
		if (this.targetType != null) {
			return ClassUtils.getPackageName(this.targetType.getRawClass()).replaceFirst("\\[L", "");
		}
		return null;
	}

	private void doAddTrustedPackages(String... packages) {
		this.typeMapper.addTrustedPackages(packages);
	}

	@Override
	public @Nullable T deserialize(String topic, Headers headers, byte @Nullable [] data) {
		if (data == null) {
			return null;
		}
		ObjectReader deserReader = null;
		JavaType javaType = null;
		if (this.typeResolver != null) {
			javaType = this.typeResolver.resolveType(topic, data, headers);
		}
		if (javaType == null && this.typeMapper.getTypePrecedence().equals(JacksonJavaTypeMapper.TypePrecedence.TYPE_ID)) {
			javaType = this.typeMapper.toJavaType(headers);
		}
		if (javaType != null) {
			deserReader = this.jsonMapper.readerFor(javaType);
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
		catch (Exception ex) {
			throw new SerializationException("Can't deserialize data  from topic [" + topic + "]", ex);
		}
	}

	@Override
	public @Nullable T deserialize(String topic, byte @Nullable [] data) {
		if (data == null) {
			return null;
		}
		ObjectReader localReader = this.reader;
		if (this.typeResolver != null) {
			JavaType javaType = this.typeResolver.resolveType(topic, data, null);
			if (javaType != null) {
				localReader = this.jsonMapper.readerFor(javaType);
			}
		}
		Assert.state(localReader != null, "No headers available and no default type provided");
		try {
			return localReader.readValue(data);
		}
		catch (Exception e) {
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
	 */
	public <X> JacksonJsonDeserializer<X> copyWithType(Class<? super X> newTargetType) {
		return copyWithType(this.jsonMapper.constructType(newTargetType));
	}

	/**
	 * Copies this deserializer with same configuration, except new target type reference is used.
	 * @param newTargetType type reference used for when type headers are missing, not null
	 * @param <X> new deserialization result type
	 * @return new instance of deserializer with type changes
	 */
	public <X> JacksonJsonDeserializer<X> copyWithType(TypeReference<? super X> newTargetType) {
		return copyWithType(this.jsonMapper.constructType(newTargetType.getType()));
	}

	/**
	 * Copies this deserializer with same configuration, except new target java type is used.
	 * @param newTargetType java type used for when type headers are missing, not null
	 * @param <X> new deserialization result type
	 * @return new instance of deserializer with type changes
	 */
	public <X> JacksonJsonDeserializer<X> copyWithType(JavaType newTargetType) {
		JacksonJsonDeserializer<X> result = new JacksonJsonDeserializer<>(newTargetType, this.jsonMapper, this.useTypeHeaders);
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
	 */
	public JacksonJsonDeserializer<T> forKeys() {
		setUseTypeMapperForKey(true);
		return this;
	}

	/**
	 * Don't remove type information headers.
	 * @return the deserializer.
	 * @see #setRemoveTypeHeaders(boolean)
	 */
	public JacksonJsonDeserializer<T> dontRemoveTypeHeaders() {
		setRemoveTypeHeaders(false);
		return this;
	}

	/**
	 * Ignore type information headers and use the configured target class.
	 * @return the deserializer.
	 * @see #setUseTypeHeaders(boolean)
	 */
	public JacksonJsonDeserializer<T> ignoreTypeHeaders() {
		setUseTypeHeaders(false);
		return this;
	}

	/**
	 * Use the supplied {@link JacksonJavaTypeMapper}.
	 * @param mapper the mapper.
	 * @return the deserializer.
	 * @see #setTypeMapper(JacksonJavaTypeMapper)
	 */
	public JacksonJsonDeserializer<T> typeMapper(JacksonJavaTypeMapper mapper) {
		setTypeMapper(mapper);
		return this;
	}

	/**
	 * Add trusted packages to the default type mapper.
	 * @param packages the packages.
	 * @return the deserializer.
	 */
	public JacksonJsonDeserializer<T> trustedPackages(String... packages) {
		try {
			this.trustedPackagesLock.lock();
			Assert.isTrue(!this.typeMapperExplicitlySet, "When using a custom type mapper, set the trusted packages there");
			this.typeMapper.addTrustedPackages(packages);
			return this;
		}
		finally {
			this.trustedPackagesLock.unlock();
		}
	}

	/**
	 * Set a {@link BiFunction} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param typeFunction the function.
	 * @return the deserializer.
	 */
	public JacksonJsonDeserializer<T> typeFunction(BiFunction<byte[], Headers, JavaType> typeFunction) {
		setTypeFunction(typeFunction);
		return this;
	}

	/**
	 * Set a {@link JsonTypeResolver} that receives the data to be deserialized and the headers
	 * and returns a JavaType.
	 * @param resolver the resolver.
	 * @return the deserializer.
	 */
	public JacksonJsonDeserializer<T> typeResolver(JacksonJsonTypeResolver resolver) {
		setTypeResolver(resolver);
		return this;
	}

	private JacksonJsonTypeResolver buildTypeResolver(String methodProperty) {
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
