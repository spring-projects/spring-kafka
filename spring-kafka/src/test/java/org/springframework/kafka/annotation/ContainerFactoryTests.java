/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
public class ContainerFactoryTests {

	@Test
	public void testConfigContainer() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setAutoStartup(false);
		factory.setConcurrency(22);
		@SuppressWarnings("unchecked")
		ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
		factory.setConsumerFactory(cf);
		factory.setPhase(42);
		factory.getContainerProperties().setAckCount(123);
		ConcurrentMessageListenerContainer<String, String> container = factory.createContainer("foo");
		assertThat(container.isAutoStartup()).isFalse();
		assertThat(container.getPhase()).isEqualTo(42);
		assertThat(container.getContainerProperties().getAckCount()).isEqualTo(123);
		assertThat(KafkaTestUtils.getPropertyValue(container, "concurrency", Integer.class)).isEqualTo(22);
	}

}
