/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.kafka.support.mapping;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.type.TypeFactory;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * Jackson 3 type mapper.
 *
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Artem Bilan
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 4.0
 */
public class DefaultJacksonJavaTypeMapper implements JacksonJavaTypeMapper, BeanClassLoaderAware {

	private static final List<String> TRUSTED_PACKAGES = List.of("java.util", "java.lang");

	private final Set<String> trustedPackages = new LinkedHashSet<>(TRUSTED_PACKAGES);

	private volatile TypePrecedence typePrecedence = TypePrecedence.INFERRED;

	/**
	 * Default header name for type information.
	 */
	public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";

	/**
	 * Default header name for container object contents type information.
	 */
	public static final String DEFAULT_CONTENT_CLASSID_FIELD_NAME = "__ContentTypeId__";

	/**
	 * Default header name for map key type information.
	 */
	public static final String DEFAULT_KEY_CLASSID_FIELD_NAME = "__KeyTypeId__";

	/**
	 * Default header name for key type information.
	 */
	public static final String KEY_DEFAULT_CLASSID_FIELD_NAME = "__Key_TypeId__";

	/**
	 * Default header name for key container object contents type information.
	 */
	public static final String KEY_DEFAULT_CONTENT_CLASSID_FIELD_NAME = "__Key_ContentTypeId__";

	/**
	 * Default header name for key map key type information.
	 */
	public static final String KEY_DEFAULT_KEY_CLASSID_FIELD_NAME = "__Key_KeyTypeId__";

	private final Map<String, Class<?>> idClassMapping = new ConcurrentHashMap<String, Class<?>>();

	private final Map<Class<?>, byte[]> classIdMapping = new ConcurrentHashMap<Class<?>, byte[]>();

	private String classIdFieldName = DEFAULT_CLASSID_FIELD_NAME;

	private String contentClassIdFieldName = DEFAULT_CONTENT_CLASSID_FIELD_NAME;

	private String keyClassIdFieldName = DEFAULT_KEY_CLASSID_FIELD_NAME;

	private @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	private TypeFactory typeFactory = TypeFactory.createDefaultInstance();

	public String getClassIdFieldName() {
		return this.classIdFieldName;
	}

	/**
	 * Configure header name for type information.
	 * @param classIdFieldName the header name.
	 */
	public void setClassIdFieldName(String classIdFieldName) {
		this.classIdFieldName = classIdFieldName;
	}

	public String getContentClassIdFieldName() {
		return this.contentClassIdFieldName;
	}

	/**
	 * Configure header name for container object contents type information.
	 * @param contentClassIdFieldName the header name.
	 */
	public void setContentClassIdFieldName(String contentClassIdFieldName) {
		this.contentClassIdFieldName = contentClassIdFieldName;
	}

	public String getKeyClassIdFieldName() {
		return this.keyClassIdFieldName;
	}

	/**
	 * Configure header name for map key type information.
	 * @param keyClassIdFieldName the header name.
	 */
	public void setKeyClassIdFieldName(String keyClassIdFieldName) {
		this.keyClassIdFieldName = keyClassIdFieldName;
	}

	public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
		this.idClassMapping.putAll(idClassMapping);
		createReverseMap();
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		this.typeFactory = this.typeFactory.withClassLoader(classLoader);
	}

	protected @Nullable ClassLoader getClassLoader() {
		return this.classLoader;
	}

	protected void addHeader(Headers headers, String headerName, Class<?> clazz) {
		if (this.classIdMapping.containsKey(clazz)) {
			headers.add(new RecordHeader(headerName, this.classIdMapping.get(clazz)));
		}
		else {
			headers.add(new RecordHeader(headerName, clazz.getName().getBytes(StandardCharsets.UTF_8)));
		}
	}

	protected String retrieveHeader(Headers headers, String headerName) {
		String classId = retrieveHeaderAsString(headers, headerName);
		if (classId == null) {
			throw new MessageConversionException(
					"failed to convert Message content. Could not resolve " + headerName + " in header");
		}
		return classId;
	}

	protected @Nullable String retrieveHeaderAsString(Headers headers, String headerName) {
		Header header = headers.lastHeader(headerName);
		if (header != null) {
			String classId = null;
			if (header.value() != null) {
				classId = new String(header.value(), StandardCharsets.UTF_8);
			}
			return classId;
		}
		return null;
	}

	private void createReverseMap() {
		this.classIdMapping.clear();
		for (Map.Entry<String, Class<?>> entry : this.idClassMapping.entrySet()) {
			String id = entry.getKey();
			Class<?> clazz = entry.getValue();
			this.classIdMapping.put(clazz, id.getBytes(StandardCharsets.UTF_8));
		}
	}

	public Map<String, Class<?>> getIdClassMapping() {
		return Collections.unmodifiableMap(this.idClassMapping);
	}

	/**
	 * Configure the TypeMapper to use default key type class.
	 * @param isKey Use key type headers if true
	 */
	public void setUseForKey(boolean isKey) {
		if (isKey) {
			setClassIdFieldName(KEY_DEFAULT_CLASSID_FIELD_NAME);
			setContentClassIdFieldName(KEY_DEFAULT_CONTENT_CLASSID_FIELD_NAME);
			setKeyClassIdFieldName(KEY_DEFAULT_KEY_CLASSID_FIELD_NAME);
		}
	}

	/**
	 * Return the precedence.
	 * @return the precedence.
	 */
	@Override
	public TypePrecedence getTypePrecedence() {
		return this.typePrecedence;
	}

	@Override
	public void setTypePrecedence(TypePrecedence typePrecedence) {
		Assert.notNull(typePrecedence, "'typePrecedence' cannot be null");
		this.typePrecedence = typePrecedence;
	}

	/**
	 * Specify a set of packages to trust during deserialization.
	 * The asterisk ({@code *}) means trust all.
	 * @param packagesToTrust the trusted Java packages for deserialization
	 */
	@Override
	public void addTrustedPackages(String... packagesToTrust) {
		if (this.trustedPackages.isEmpty()) {
			return;
		}
		if (packagesToTrust != null) {
			for (String trusted : packagesToTrust) {
				if ("*".equals(trusted)) {
					this.trustedPackages.clear();
					break;
				}
				else {
					this.trustedPackages.add(trusted);
				}
			}
		}
	}

	@Override
	public @Nullable JavaType toJavaType(Headers headers) {
		String typeIdHeader = retrieveHeaderAsString(headers, getClassIdFieldName());

		if (typeIdHeader != null) {

			JavaType classType = getClassIdType(typeIdHeader);
			if (!classType.isContainerType() || classType.isArrayType()) {
				return classType;
			}

			JavaType contentClassType = getClassIdType(retrieveHeader(headers, getContentClassIdFieldName()));
			if (classType.getKeyType() == null) {
				return this.typeFactory.constructCollectionLikeType(classType.getRawClass(), contentClassType);
			}

			JavaType keyClassType = getClassIdType(retrieveHeader(headers, getKeyClassIdFieldName()));
			return this.typeFactory.constructMapLikeType(classType.getRawClass(), keyClassType, contentClassType);
		}

		return null;
	}

	private JavaType getClassIdType(String classId) {
		if (getIdClassMapping().containsKey(classId)) {
			return this.typeFactory.constructType(getIdClassMapping().get(classId));
		}
		else {
			try {
				if (!isTrustedPackage(classId)) {
					throw new IllegalArgumentException("The class '" + classId
							+ "' is not in the trusted packages: "
							+ this.trustedPackages + ". "
							+ "If you believe this class is safe to deserialize, please provide its name. "
							+ "If the serialization is only done by a trusted source, you can also enable "
							+ "trust all (*).");
				}
				else {
					return this.typeFactory
							.constructType(ClassUtils.forName(classId, getClassLoader()));
				}
			}
			catch (ClassNotFoundException e) {
				throw new MessageConversionException("failed to resolve class name. Class not found ["
						+ classId + "]", e);
			}
			catch (LinkageError e) {
				throw new MessageConversionException("failed to resolve class name. Linkage error ["
						+ classId + "]", e);
			}
		}
	}

	private boolean isTrustedPackage(String requestedType) {
		if (!this.trustedPackages.isEmpty()) {
			String packageName = ClassUtils.getPackageName(requestedType).replaceFirst("\\[L", "");
			for (String trustedPackage : this.trustedPackages) {
				if (PatternMatchUtils.simpleMatch(trustedPackage, packageName)) {
					return true;
				}
			}
			return false;
		}
		return true;
	}

	@Override
	public void fromJavaType(JavaType javaType, Headers headers) {
		String classIdFieldName = getClassIdFieldName();
		if (headers.lastHeader(classIdFieldName) != null) {
			removeHeaders(headers);
		}

		addHeader(headers, classIdFieldName, javaType.getRawClass());

		if (javaType.isContainerType() && !javaType.isArrayType()) {
			addHeader(headers, getContentClassIdFieldName(), javaType.getContentType().getRawClass());
		}

		if (javaType.getKeyType() != null) {
			addHeader(headers, getKeyClassIdFieldName(), javaType.getKeyType().getRawClass());
		}
	}

	@Override
	public void fromClass(Class<?> clazz, Headers headers) {
		fromJavaType(this.typeFactory.constructType(clazz), headers);

	}

	@Override
	public @Nullable Class<?> toClass(Headers headers) {
		JavaType javaType = toJavaType(headers);
		return javaType == null ? null : javaType.getRawClass();
	}

	@Override
	public void removeHeaders(Headers headers) {
		try {
			headers.remove(getClassIdFieldName());
			headers.remove(getContentClassIdFieldName());
			headers.remove(getKeyClassIdFieldName());
		}
		catch (Exception e) { // NOSONAR
			// NOSONAR
		}
	}
}
