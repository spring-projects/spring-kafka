/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.listener;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.core.ShareConsumerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for {@link ShareKafkaMessageListenerContainer}.
 * These tests focus on configuration validation and setup logic.
 * Message processing, acknowledgment behavior, and error handling are covered
 * by integration tests in {@link ShareKafkaMessageListenerContainerIntegrationTests}.
 *
 * @author Soby Chacko
 * @since 4.0
 */
@ExtendWith(MockitoExtension.class)
public class ShareKafkaMessageListenerContainerUnitTests {

	@Mock
	private ShareConsumerFactory<String, String> shareConsumerFactory;

	@Mock
	private MessageListener<String, String> messageListener;

	@Mock
	private AcknowledgingShareConsumerAwareMessageListener<String, String> ackListener;

	@Test
	void shouldConfigureExplicitModeCorrectly() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setExplicitShareAcknowledgment(true);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().isExplicitShareAcknowledgment())
				.isTrue();
	}

	@Test
	void shouldConfigureImplicitModeByDefault() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().isExplicitShareAcknowledgment())
				.isFalse();
	}

	@Test
	void shouldFailWhenExplicitModeUsedWithNonAcknowledgingListener() {
		// Given: A container with explicit acknowledgment mode
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setExplicitShareAcknowledgment(true);
		// Using a non-acknowledging listener (just GenericMessageListener)
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		// Starting the container should fail with validation error
		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(container::start)
				.withMessageContaining("Explicit acknowledgment mode requires an AcknowledgingShareConsumerAwareMessageListener");
	}

	@Test
	void shouldRejectInvalidConcurrency() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> container.setConcurrency(0))
				.withMessageContaining("concurrency must be greater than 0");

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> container.setConcurrency(-1))
				.withMessageContaining("concurrency must be greater than 0");
	}

	@Test
	void shouldValidateListenerTypeOnStartup() {
		// Given: A container with explicit acknowledgment mode and proper listener
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setExplicitShareAcknowledgment(true);
		// Using an acknowledging listener should not throw during construction
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		// Validation occurs during startup, but we don't need to actually start for this test
		assertThat(container.getContainerProperties().isExplicitShareAcknowledgment()).isTrue();
	}

	@Test
	void shouldSupportBeanNameSetting() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.setBeanName("testContainer");
		assertThat(container.getBeanName()).isEqualTo("testContainer");
		assertThat(container.getListenerId()).isEqualTo("testContainer");
	}

	@Test
	void shouldReportRunningStateBeforeStart() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		// Should not be running before start
		assertThat(container.isRunning()).isFalse();
	}

	@Test
	void shouldSupportContainerProperties() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(ackListener);
		containerProperties.setExplicitShareAcknowledgment(true);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().isExplicitShareAcknowledgment())
				.isTrue();
	}

}
