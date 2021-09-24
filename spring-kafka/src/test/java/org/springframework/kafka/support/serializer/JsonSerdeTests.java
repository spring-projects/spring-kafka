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

import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.mapping.AbstractJavaTypeMapper;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class JsonSerdeTests {

	@Test
	void noTypeInfo() {
		JsonSerde<String> serde = new JsonSerde<>(String.class)
				.forKeys()
				.noTypeInfo()
				.ignoreTypeHeaders()
				.dontRemoveTypeHeaders();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonSerializer.typeMapper.classIdFieldName"))
				.isEqualTo(AbstractJavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME);
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.typeMapper.classIdFieldName"))
				.isEqualTo(AbstractJavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME);
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonSerializer.addTypeInfo", Boolean.class)).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.useTypeHeaders", Boolean.class)).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.removeTypeHeaders", Boolean.class))
				.isFalse();
		serde.close();
	}

}
