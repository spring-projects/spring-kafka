/*
 * Copyright 2023-present the original author or authors.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

import org.apache.kafka.common.header.Header;

/**
 * @author Gary Russell
 * @author Soby Chacko
 * @since 2.9.11
 *
 */
public final class SerializationTestUtils {

	private SerializationTestUtils() {
	}

	public static Header deserializationHeader(String key, byte[] value) {
		return new DeserializationExceptionHeader(key, value);
	}

	public static byte[] header(boolean isKey) {
		return header(createDeserEx(isKey));
	}

	public static DeserializationException createDeserEx(boolean isKey) {
		return new DeserializationException(
				isKey ? "testK" : "testV",
				isKey ? "key".getBytes() : "value".getBytes(), isKey, null);
	}

	public static byte[] header(DeserializationException deserEx) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(baos).writeObject(deserEx);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return baos.toByteArray();
	}

}
