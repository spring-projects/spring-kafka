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

import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.KafkaUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Gary Russell
 * @since 2.9.11
 *
 */
public class SerializationUtilsTests {

	@SuppressWarnings("unchecked")
	@Test
	void foreignDeserEx() {
		RecordHeaders headers = new RecordHeaders(
				List.of(new RecordHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, "junk".getBytes())));
		ConsumerRecord<String, String> rec = mock(ConsumerRecord.class);
		willReturn(headers).given(rec).headers();
		given(rec.topic()).willReturn("foo");
		given(rec.partition()).willReturn(1);
		given(rec.offset()).willReturn(0L);
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog(getClass())));
		ArgumentCaptor<Supplier<String>> captor = ArgumentCaptor.forClass(Supplier.class);
		willAnswer(inv -> null).given(logger).warn(captor.capture());
		assertThat(SerializationUtils.getExceptionFromHeader(rec,
				KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, logger)).isNull();
		assertThat(captor.getValue().get())
				.isEqualTo("Foreign deserialization exception header in (foo-1@0) ignored; possible attack?");
	}

}
