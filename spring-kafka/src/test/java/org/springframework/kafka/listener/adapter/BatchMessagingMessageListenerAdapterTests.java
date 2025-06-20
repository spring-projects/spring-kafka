/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Wang Zhiyang
 * @since 2.2.5
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class BatchMessagingMessageListenerAdapterTests {

	@SuppressWarnings("unchecked")
	@Test
	public void testKafkaNullInList(@Autowired KafkaListenerEndpointRegistry registry, @Autowired Foo foo) {
		BatchMessagingMessageListenerAdapter<String, String> adapter =
				(BatchMessagingMessageListenerAdapter<String, String>) registry
					.getListenerContainer("foo").getContainerProperties().getMessageListener();
		KafkaUtils.setConsumerGroupId("test.group");
		adapter.onMessage(Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, null, null)), null, null);
		assertThat(foo.value).isNull();
		assertThat(foo.group).isEqualTo("test.group");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFutureResult(@Autowired KafkaListenerEndpointRegistry registry,
			@Autowired BatchFuture batchFuture) {

		BatchMessagingMessageListenerAdapter<String, String> adapter =
				spy((BatchMessagingMessageListenerAdapter<String, String>) registry
						.getListenerContainer("batchFuture").getContainerProperties().getMessageListener());
		KafkaUtils.setConsumerGroupId("test.group.future");
		List<ConsumerRecord<String, String>> list = new ArrayList<>();
		list.add(new ConsumerRecord<>("batchFuture", 0, 0L, null, "future_1"));
		list.add(new ConsumerRecord<>("batchFuture", 0, 1L, null, "future_2"));
		list.add(new ConsumerRecord<>("batchFuture", 1, 0L, null, "future_3"));
		adapter.onMessage(list, null, null);
		assertThat(batchFuture.group).isEqualTo("test.group.future");
		verify(adapter, times(1)).asyncSuccess(any(), any(), any(), anyBoolean());
		verify(adapter, times(1)).acknowledge(any());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMonoResult(@Autowired KafkaListenerEndpointRegistry registry, @Autowired BatchMono batchMono) {

		BatchMessagingMessageListenerAdapter<String, String> adapter =
				spy((BatchMessagingMessageListenerAdapter<String, String>) registry
						.getListenerContainer("batchMono").getContainerProperties().getMessageListener());
		KafkaUtils.setConsumerGroupId("test.group.mono");
		List<ConsumerRecord<String, String>> list = new ArrayList<>();
		list.add(new ConsumerRecord<>("batchMono", 0, 0L, null, "mono_1"));
		list.add(new ConsumerRecord<>("batchMono", 0, 1L, null, "mono_2"));
		adapter.onMessage(list, null, null);
		assertThat(batchMono.group).isEqualTo("test.group.mono");
		verify(adapter, times(1)).asyncSuccess(any(), any(), any(), anyBoolean());
		verify(adapter, times(1)).acknowledge(any());
	}

	public static class Foo {

		public volatile String value = "someValue";

		public volatile String group;

		@KafkaListener(id = "foo", topics = "foo", autoStartup = "false")
		public void listen(List<String> list, @Header(KafkaHeaders.GROUP_ID) String groupId) {
			list.forEach(s -> this.value = s);
			this.group = groupId;
		}

	}

	public static class BatchFuture {

		public volatile String group;

		@KafkaListener(id = "batchFuture", topics = "batchFuture", autoStartup = "false")
		public CompletableFuture<String> listen(List<String> list, @Header(KafkaHeaders.GROUP_ID) String groupId) {

			this.group = groupId;
			CompletableFuture<String> future = new CompletableFuture<>();
			future.complete("processed: " + list.size());
			return future;
		}

	}

	public static class BatchMono {

		public volatile String value = "someValue";

		public volatile String group;

		@KafkaListener(id = "batchMono", topics = "batchMono", autoStartup = "false")
		public Mono<Integer> listen(List<String> list, @Header(KafkaHeaders.GROUP_ID) String groupId) {

			this.group = groupId;
			return Mono.just(list.size());
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public Foo foo() {
			return new Foo();
		}

		@Bean
		public BatchFuture batchFuture() {
			return new BatchFuture();
		}

		@Bean
		public BatchMono batchMono() {
			return new BatchMono();
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			return mock(ConsumerFactory.class);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			return factory;
		}

	}

}
