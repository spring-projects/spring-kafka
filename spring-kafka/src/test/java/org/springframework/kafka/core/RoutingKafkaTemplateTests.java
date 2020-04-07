/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * @author Gary Russell
 * @since 2.5
 *
 */
public class RoutingKafkaTemplateTests {

	@SuppressWarnings({ "unchecked" })
	@Test
	public void routing() {
		Producer<Object, Object> p1 = mock(Producer.class);
		given(p1.send(any(), any())).willReturn(new SettableListenableFuture<>());
		Producer<Object, Object> p2 = mock(Producer.class);
		given(p2.send(any(), any())).willReturn(new SettableListenableFuture<>());
		ProducerFactory<Object, Object> pf1 = mock(ProducerFactory.class);
		ProducerFactory<Object, Object> pf2 = mock(ProducerFactory.class);
		given(pf1.createProducer()).willReturn(p1);
		given(pf2.createProducer()).willReturn(p2);
		Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
		map.put(Pattern.compile("fo.*"), pf1);
		map.put(Pattern.compile(".*"), pf2);
		RoutingKafkaTemplate template = new RoutingKafkaTemplate(map);
		template.send("foo", "test");
		template.send("bar", "test");
		template.send("foo", "test");
		template.send("bar", "test");
		verify(p1, times(2)).send(any(), any());
		verify(p2, times(2)).send(any(), any());
		assertThat(KafkaTestUtils.getPropertyValue(template, "factoryMap", Map.class)).hasSize(2);
	}

}
