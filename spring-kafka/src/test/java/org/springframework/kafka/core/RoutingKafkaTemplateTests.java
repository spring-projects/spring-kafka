/*
 * Copyright 2020-present the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Nathan Xu
 * @author Minju Kim
 *
 * @since 2.5
 *
 */
public class RoutingKafkaTemplateTests {

	@SuppressWarnings("unchecked")
	@Test
	public void routing() {
		Producer<Object, Object> p1 = mock(Producer.class);
		given(p1.send(any(), any())).willReturn(new CompletableFuture<>());
		Producer<Object, Object> p2 = mock(Producer.class);
		given(p2.send(any(), any())).willReturn(new CompletableFuture<>());
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

	@Test
	public void partitionsForRoutesByTopic() {
		Producer<Object, Object> ordersProducer = mock();
		List<PartitionInfo> ordersPartitions = List.of(new PartitionInfo("orders", 0, null, null, null));
		given(ordersProducer.partitionsFor("orders")).willReturn(ordersPartitions);
		Producer<Object, Object> defaultProducer = mock();
		List<PartitionInfo> paymentsPartitions = List.of(new PartitionInfo("payments", 0, null, null, null));
		given(defaultProducer.partitionsFor("payments")).willReturn(paymentsPartitions);
		ProducerFactory<Object, Object> ordersProducerFactory = mock();
		ProducerFactory<Object, Object> defaultProducerFactory = mock();
		given(ordersProducerFactory.createProducer()).willReturn(ordersProducer);
		given(defaultProducerFactory.createProducer()).willReturn(defaultProducer);
		Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
		map.put(Pattern.compile("orders.*"), ordersProducerFactory);
		map.put(Pattern.compile(".*"), defaultProducerFactory);
		RoutingKafkaTemplate template = new RoutingKafkaTemplate(map);
		assertThat(template.partitionsFor("orders")).isSameAs(ordersPartitions);
		assertThat(template.partitionsFor("payments")).isSameAs(paymentsPartitions);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testTransactionalFactoriesNotSupported() {
		ProducerFactory<Object, Object> pf1 = mock(ProducerFactory.class);
		given(pf1.transactionCapable()).willReturn(false);
		ProducerFactory<Object, Object> pf2 = mock(ProducerFactory.class);
		given(pf2.transactionCapable()).willReturn(true);
		Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
		map.put(Pattern.compile("fo.*"), pf1);
		map.put(Pattern.compile(".*"), pf2);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RoutingKafkaTemplate(map))
				.withMessage("Transactional factories are not supported");
	}

}
