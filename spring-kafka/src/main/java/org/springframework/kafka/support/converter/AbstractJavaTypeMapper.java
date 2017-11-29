/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.support.converter;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.ClassUtils;

/**
 * Abstract type mapper.
 *
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 *
 * @since 2.1
 */
public abstract class AbstractJavaTypeMapper implements BeanClassLoaderAware {

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

	private final Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();

	private final Map<Class<?>, byte[]> classIdMapping = new HashMap<Class<?>, byte[]>();

	private ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	public String getClassIdFieldName() {
		return DEFAULT_CLASSID_FIELD_NAME;
	}

	public String getContentClassIdFieldName() {
		return DEFAULT_CONTENT_CLASSID_FIELD_NAME;
	}

	public String getKeyClassIdFieldName() {
		return DEFAULT_KEY_CLASSID_FIELD_NAME;
	}

	public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
		this.idClassMapping.putAll(idClassMapping);
		createReverseMap();
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	protected ClassLoader getClassLoader() {
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

	protected String retrieveHeaderAsString(Headers headers, String headerName) {
		Iterator<Header> headerValues = headers.headers(headerName).iterator();
		if (headerValues.hasNext()) {
			Header headerValue = headerValues.next();
			String classId = null;
			if (headerValue.value() != null) {
				classId = new String(headerValue.value(), StandardCharsets.UTF_8);
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

}
