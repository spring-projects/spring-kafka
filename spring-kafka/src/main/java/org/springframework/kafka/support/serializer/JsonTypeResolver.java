/*
 * Copyright 2020 the original author or authors.
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

import org.apache.kafka.common.header.Headers;

import com.fasterxml.jackson.databind.JavaType;

/**
 * Determine the {@link JavaType} from the topic/data/headers.
 *
 * @author Gary Russell
 * @since 2.5.3
 *
 */
@FunctionalInterface
public interface JsonTypeResolver {

	/**
	 * Determine the type.
	 * @param topic the topic.
	 * @param data the serialized data.
	 * @param headers the headers.
	 * @return the type.
	 */
	JavaType resolveType(String topic, byte[] data, Headers headers);

}
