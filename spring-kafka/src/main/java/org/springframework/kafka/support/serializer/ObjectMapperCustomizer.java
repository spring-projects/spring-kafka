/*
 * Copyright 2020-2021 the original author or authors.
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

/**
 * Provide hook to configure the ObjectMapper used in Json(De)Serializer.
 *
 * @author Carl Nygard
 * @since 2.7.1
 *
 */
@FunctionalInterface
public interface ObjectMapperCustomizer {

	/**
	 * Customize (or replace) the ObjectMapper used in serialization.
	 * @param objectMapper the ObjectMapper instance.
	 * @return the configured (or possibly new) ObjectMapper instance.
	 */
	ObjectMapper customizeObjectMapper(ObjectMapper objectMapper);
}
