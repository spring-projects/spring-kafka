/*
 * Copyright 2022-2024 the original author or authors.
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

package org.springframework.kafka.config;

import org.junit.jupiter.api.Test;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.8.9
 *
 */
public class KafkaListenerEndpointRegistryTests {

	@Test
	void unregister() {
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		KafkaListenerEndpoint endpoint = mock(KafkaListenerEndpoint.class);
		@SuppressWarnings("unchecked")
		KafkaListenerContainerFactory<MessageListenerContainer> factory = mock(KafkaListenerContainerFactory.class);
		given(endpoint.getId()).willReturn("foo");
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(factory.createListenerContainer(endpoint)).willReturn(container);
		registry.registerListenerContainer(endpoint, factory);
		MessageListenerContainer unregistered = registry.unregisterListenerContainer("foo");
		assertThat(unregistered).isSameAs(container);
		registry.registerListenerContainer(endpoint, factory);
		assertThat(unregistered).isSameAs(container);
	}

	@Test
	void verifyUnregisteredListenerContainer() {
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		ConcurrentMessageListenerContainer<?, ?> listenerContainerMock = mock(ConcurrentMessageListenerContainer.class);
		given(listenerContainerMock.getListenerId()).willReturn("testListenerContainer");
		applicationContext.registerBean(ConcurrentMessageListenerContainer.class, () -> listenerContainerMock);
		applicationContext.refresh();
		registry.setApplicationContext(applicationContext);
		// Lazy-load from application context
		assertThat(registry.getUnregisteredListenerContainer("testListenerContainer")).isNotNull();
		// From internal map
		assertThat(registry.getUnregisteredListenerContainer("testListenerContainer")).isNotNull();
	}

}
