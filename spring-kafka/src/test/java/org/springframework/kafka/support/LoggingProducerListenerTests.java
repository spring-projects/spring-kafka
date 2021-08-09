/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.5.16
 *
 */
public class LoggingProducerListenerTests {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void noBytesInLog() {
		LoggingProducerListener pl = new LoggingProducerListener();
		LogAccessor logger = (LogAccessor) spy(KafkaTestUtils.getPropertyValue(pl, "logger"));
		new DirectFieldAccessor(pl).setPropertyValue("logger", logger);
		AtomicReference<String> string = new AtomicReference<>();
		willAnswer(inv -> {
			Supplier<String> stringer = inv.getArgument(1);
			string.set(stringer.get());
			return null;
		}).given(logger).error(any(), any(Supplier.class));
		pl.onError(new ProducerRecord("foo", 0, new byte[3], new byte[1111]), null,
				new RuntimeException());
		assertThat(string.get()).contains("byte[3]");
		assertThat(string.get()).contains("byte[1111]");
	}

}
