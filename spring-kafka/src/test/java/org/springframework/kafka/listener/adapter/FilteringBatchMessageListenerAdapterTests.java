/*
 * Copyright 2017-2023 the original author or authors.
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


import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Nathan Xu
 * @since 3.1
 */
@SpringJUnitConfig
@DirtiesContext
public class FilteringBatchMessageListenerAdapterTests {

	@Test
	@SuppressWarnings( "unchecked" )
	void testEmptyFilteringResultListenerNotInvoked(@Autowired KafkaListenerEndpointRegistry registry, @Autowired Listener listener) {
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<>((BatchMessagingMessageListenerAdapter<String, String>) registry
						.getListenerContainer("foo").getContainerProperties().getMessageListener(), r -> false);
		ConsumerRecord<String, String> record = new ConsumerRecord<>( "foo", 0, 0L, null, "bar");
		List<ConsumerRecord<String, String>> list = Collections.singletonList(record);
		adapter.onMessage(list);
		Listener spy = spy(listener);
		verify(spy, never()).listen(any(List.class));
	}
	public static class Listener {

		@KafkaListener(id = "foo", topics = "foo", autoStartup = "false")
		public void listen(List<Foo> list) {
		}

	}

	public static class Foo {

		private String bar;

		public Foo() {
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public int hashCode() {
			return Objects.hash( bar);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Foo other = (Foo) obj;
			return Objects.equals(this.bar, other.bar);
		}

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public Listener foo() {
			return new Listener();
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			return mock( ConsumerFactory.class);
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
