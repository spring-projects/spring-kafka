/*
 * Copyright 2019-2021 the original author or authors.
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

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class StringOrBytesSerializerTests {

	@Test
	void test() {
		StringOrBytesSerializer serializer = new StringOrBytesSerializer();
		String string = "foo";
		byte[] out = serializer.serialize("x", string);
		assertThat(out).isEqualTo("foo".getBytes());
		byte[] byteArray = "bar".getBytes();
		out = serializer.serialize("x", byteArray);
		assertThat(out).isEqualTo("bar".getBytes());
		Bytes bytes = Bytes.wrap("baz".getBytes());
		out = serializer.serialize("x", bytes);
		assertThat(out).isEqualTo("baz".getBytes());
		assertThat(KafkaTestUtils.getPropertyValue(serializer, "stringSerializer.encoding")).isEqualTo("UTF-8");
		Map<String, Object> configs = Collections.singletonMap("serializer.encoding", "UTF-16");
		serializer.configure(configs, false);
		assertThat(KafkaTestUtils.getPropertyValue(serializer, "stringSerializer.encoding")).isEqualTo("UTF-16");
	}

}
