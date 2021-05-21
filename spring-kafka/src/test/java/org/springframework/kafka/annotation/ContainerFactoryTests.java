/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.config.AbstractKafkaListenerEndpoint;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
public class ContainerFactoryTests {

	@Test
	void testConfigContainer() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setAutoStartup(false);
		factory.setConcurrency(22);
		@SuppressWarnings("unchecked")
		ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
		factory.setConsumerFactory(cf);
		factory.setPhase(42);
		factory.getContainerProperties().setAckCount(123);
		AtomicBoolean customized = new AtomicBoolean();
		factory.setContainerCustomizer(container -> customized.set(true));
		ConcurrentMessageListenerContainer<String, String> container = factory.createContainer("foo");
		assertThat(container.isAutoStartup()).isFalse();
		assertThat(container.getPhase()).isEqualTo(42);
		assertThat(container.getContainerProperties().getAckCount()).isEqualTo(123);
		assertThat(KafkaTestUtils.getPropertyValue(container, "concurrency", Integer.class)).isEqualTo(22);
		assertThat(customized).isTrue();
		ConcurrentMessageListenerContainer<String, String> container2 = factory.createContainer("foo");
		assertThat(container.getContainerProperties().getKafkaConsumerProperties())
				.isNotSameAs(container2.getContainerProperties().getKafkaConsumerProperties());
	}

	@SuppressWarnings("unchecked")
	@Test
	void clientIdAndGroupIdTransferred() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setClientId("myClientId");
		factory.getContainerProperties().setGroupId("myGroup");
		factory.setConsumerFactory(mock(ConsumerFactory.class));
		AbstractKafkaListenerEndpoint<String, String> endpoint = new AbstractKafkaListenerEndpoint<String, String>() {

			@Override
			protected MessagingMessageListenerAdapter<String, String> createMessageListener(
					MessageListenerContainer container, MessageConverter messageConverter) {

				RecordMessagingMessageListenerAdapter<String, String> adapter =
						new RecordMessagingMessageListenerAdapter<String, String>(null, null);
				return adapter;
			}

		};
		endpoint.setTopics("test");
		endpoint.setClientIdPrefix("");
		endpoint.setGroupId("");
		ConcurrentMessageListenerContainer<String, String> container = factory.createListenerContainer(
				endpoint);
		assertThat(container.getContainerProperties().getClientId()).isEqualTo("myClientId");
		assertThat(container.getContainerProperties().getGroupId()).isEqualTo("myGroup");
	}

}
