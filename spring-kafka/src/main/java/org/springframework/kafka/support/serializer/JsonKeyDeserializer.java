/*
 * Copyright 2015-2025 the original author or authors.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.mapping.DefaultJackson2JavaKeyTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;

/**
 * Same as JsonDeserializer only the classIdFieldName is other because using
 * DefaultJackson2JavaKeyTypeMapper.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Popovics Boglarka
 */
public class JsonKeyDeserializer<T> extends JsonDeserializer<T> {

	@Override
	public Jackson2JavaTypeMapper getTypeMapper() {
		return new DefaultJackson2JavaKeyTypeMapper();
	}

	/**
	 * Construct an instance with a default {@link ObjectMapper}.
	 */
	public JsonKeyDeserializer() {
		super((Class<T>) null, true);
	}

	/**
	 * Construct an instance with the provided {@link ObjectMapper}.
	 *
	 * @param objectMapper a custom object mapper.
	 */
	public JsonKeyDeserializer(ObjectMapper objectMapper) {
		super((Class<T>) null, objectMapper, true);
		setTypeMapper(new DefaultJackson2JavaKeyTypeMapper());
	}

	/**
	 * Construct an instance with the provided target type.
	 *
	 * @param targetType the target type to use if no type info headers are present.
	 */
	public JsonKeyDeserializer(@Nullable Class<? super T> targetType) {
		super(targetType);
		setTypeMapper(new DefaultJackson2JavaKeyTypeMapper());
	}

}
