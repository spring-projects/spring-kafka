/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;

/**
 * @author Gary Russell
 * @since 2.2.11
 */
public class FailWhenPayloadIsFailDeserializer implements ExtendedDeserializer<String> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public String deserialize(String topic, byte[] data) {
		return new String(data);
	}

	@Override
	public void close() {
	}

	@Override
	public String deserialize(String topic, Headers headers, byte[] data) {
		String string = new String(data);
		if ("fail".equals(string)) {
			throw new RuntimeException("fail");
		}
		return string;
	}

}
