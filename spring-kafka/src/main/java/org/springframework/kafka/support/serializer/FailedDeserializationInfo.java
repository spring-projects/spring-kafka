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

import java.util.Arrays;

import org.apache.kafka.common.header.Headers;

/**
 * Class containing all the contextual information around a deserialization error.
 *
 * @author Victor Perez Rey
 * @author Artem Bilan
 *
 * @since 2.2.8
 */
public class FailedDeserializationInfo {

	private final String topic;

	private final Headers headers;

	private final byte[] data;

	private final boolean isForKey;

	private final Exception exception;

	/**
	 * Construct an instance with the contextual information.
	 * @param topic    topic associated with the data.
	 * @param headers  headers associated with the record; may be empty.
	 * @param data     serialized bytes; may be null.
	 * @param isForKey true for a key deserializer, false otherwise.
	 * @param exception exception causing the deserialization error.
	 */
	public FailedDeserializationInfo(String topic, Headers headers, byte[] data, boolean isForKey,
			Exception exception) {

		this.topic = topic;
		this.headers = headers;
		this.data = Arrays.copyOf(data, data.length);
		this.isForKey = isForKey;
		this.exception = exception;
	}

	public String getTopic() {
		return this.topic;
	}

	public Headers getHeaders() {
		return this.headers;
	}

	public byte[] getData() {
		return Arrays.copyOf(this.data, this.data.length);
	}

	public boolean isForKey() {
		return this.isForKey;
	}

	public Exception getException() {
		return this.exception;
	}

	@Override
	public String toString() {
		return "FailedDeserializationInfo{" +
				"topic='" + this.topic + '\'' +
				", headers=" + this.headers +
				", data=" + Arrays.toString(this.data) +
				", isForKey=" + this.isForKey +
				", exception=" + this.exception +
				'}';
	}

}
