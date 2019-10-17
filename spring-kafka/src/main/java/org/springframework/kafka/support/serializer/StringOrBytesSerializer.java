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

package org.springframework.kafka.support.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

/**
 * A serializer that can handle {@code byte[]}, {@link Bytes} and {@link String}.
 * Convenient when used with one of the Json message converters.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class StringOrBytesSerializer implements Serializer<Object> {

	private final StringSerializer stringSerializer = new StringSerializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.stringSerializer.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		if (data instanceof byte[]) {
			return (byte[]) data;
		}
		else if (data instanceof Bytes) {
			return ((Bytes) data).get();
		}
		else if (data instanceof String) {
			return this.stringSerializer.serialize(topic, (String) data);
		}
		else if (data == null) {
			return null;
		}
		else {
			throw new IllegalStateException("This serializer can only handle byte[], Bytes or String values");
		}
	}

	@Override
	public void close() {
		this.stringSerializer.close();
	}

}
