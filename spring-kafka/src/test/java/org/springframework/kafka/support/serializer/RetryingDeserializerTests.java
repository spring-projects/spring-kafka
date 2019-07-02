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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class RetryingDeserializerTests {

	@Test
	void testRetry() {
		Deser delegate = new Deser();
		RetryingDeserializer<String> rdes = new RetryingDeserializer<>(delegate, new RetryTemplate());
		assertThat(rdes.deserialize("foo", "bar".getBytes())).isEqualTo("bar");
		assertThat(delegate.n).isEqualTo(3);
		delegate.n = 0;
		assertThat(rdes.deserialize("foo", new RecordHeaders(), "bar".getBytes())).isEqualTo("bar");
		assertThat(delegate.n).isEqualTo(3);
		rdes.close();
	}

	public static class Deser implements Deserializer<String> {

		int n;

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public String deserialize(String topic, byte[] data) {
			if (n++ < 2) {
				throw new RuntimeException();
			}
			return new String(data);
		}

		@Override
		public String deserialize(String topic, Headers headers, byte[] data) {
			if (n++ < 2) {
				throw new RuntimeException();
			}
			return new String(data);
		}

		@Override
		public void close() {
			// empty
		}

	}

}
