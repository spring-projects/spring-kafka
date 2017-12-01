/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.web.JsonPath;
import org.springframework.kafka.support.KafkaNull;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class ProjectingMessageConverterTests {

	static final String STRING_PAYLOAD = "{ \"username\" : \"SomeUsername\", \"user\" : { \"name\" : \"SomeName\"}}";
	static final byte[] BYTE_ARRAY_PAYLOAD = STRING_PAYLOAD.getBytes(StandardCharsets.UTF_8);

	ProjectingMessageConverter converter = new ProjectingMessageConverter(new ObjectMapper());

	@Mock
	ConsumerRecord<?, ?> record;

	public @Rule ExpectedException exception = ExpectedException.none();

	@Test
	public void rejectsNullObjectMapper() {

		exception.expect(IllegalArgumentException.class);

		new ProjectingMessageConverter(null);
	}

	@Test
	public void returnsKafkaNullForNullPayload() {

		doReturn(null).when(record).value();

		assertThat(converter.extractAndConvertValue(record, Object.class)).isEqualTo(KafkaNull.INSTANCE);
	}

	@Test
	public void createsProjectedPayloadForInterface() {

		assertProjectionProxy(STRING_PAYLOAD);
		assertProjectionProxy(BYTE_ARRAY_PAYLOAD);
	}

	@Test
	public void usesJacksonToCreatePayloadForClass() {

		assertSimpleObject(STRING_PAYLOAD);
		assertSimpleObject(BYTE_ARRAY_PAYLOAD);
	}

	@Test
	public void rejectsInvalidPayload() {

		exception.expect(ConversionException.class);
		exception.expectMessage(Object.class.getName());

		assertProjectionProxy(new Object());
	}

	private void assertProjectionProxy(Object payload) {

		doReturn(payload).when(record).value();

		Object value = converter.extractAndConvertValue(record, Sample.class);

		assertThat(value).isInstanceOf(Sample.class);

		Sample sample = (Sample) value;

		assertThat(sample.getName()).isEqualTo("SomeName");
		assertThat(sample.getUsername()).isEqualTo("SomeUsername");
	}

	private void assertSimpleObject(Object payload) {

		doReturn(payload).when(record).value();

		Object value = converter.extractAndConvertValue(record, AnotherSample.class);

		assertThat(value).isInstanceOf(AnotherSample.class);

		AnotherSample sample = (AnotherSample) value;

		assertThat(sample.user.name).isEqualTo("SomeName");
		assertThat(sample.username).isEqualTo("SomeUsername");
	}

	interface Sample {

		String getUsername();

		@JsonPath("$.user.name")
		String getName();
	}

	public static class AnotherSample {

		public String username;
		public User user;

		public static class User {
			public String name;
		}
	}
}
