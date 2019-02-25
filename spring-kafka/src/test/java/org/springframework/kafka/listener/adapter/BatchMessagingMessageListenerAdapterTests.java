/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

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

/**
 * @author Gary Russell
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
		adapter.onMessage(Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, null, null)), null, null);
		assertThat(foo.value).isNull();
	}

	public static class Foo {

		public String value = "someValue";

		@KafkaListener(id = "foo", topics = "foo", autoStartup = "false")
		public void listen(List<String> list) {
			list.forEach(s -> {
				this.value = s;
			});
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public Foo foo() {
			return new Foo();
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			return consumerFactory;
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
