/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class JsonSerdeTests {

	@Test
	void noTypeInfo() {
		JacksonJsonSerde<String> serde = new JacksonJsonSerde<>(String.class)
				.forKeys()
				.noTypeInfo()
				.ignoreTypeHeaders()
				.dontRemoveTypeHeaders();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonSerializer.typeMapper.classIdFieldName"))
				.isEqualTo(DefaultJacksonJavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME);
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.typeMapper.classIdFieldName"))
				.isEqualTo(DefaultJacksonJavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME);
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonSerializer.addTypeInfo", Boolean.class)).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.useTypeHeaders", Boolean.class)).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.removeTypeHeaders", Boolean.class))
				.isFalse();
		serde.close();
	}

}
