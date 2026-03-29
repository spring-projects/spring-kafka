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

import java.util.Map;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.ShareAckMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
	void shouldDefaultToExplicitAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.EXPLICIT);
	}

	@Test
	void shouldConfigureImplicitAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.IMPLICIT);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.IMPLICIT);
	}

	@Test
	void shouldConfigureManualAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.MANUAL);
	}

	@Test
	void shouldFailWhenManualAckModeUsedWithNonAcknowledgingListener() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(container::start)
				.withMessageContaining("ShareAckMode.MANUAL requires an AcknowledgingShareConsumerAwareMessageListener");
	}

	@Test
	void shouldNotFailWhenManualAckModeUsedWithAcknowledgingListener() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.MANUAL);
	}

	@Test
	@SuppressWarnings("deprecation")
	void deprecatedSetExplicitShareAcknowledgmentShouldDelegateToShareAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");

		containerProperties.setExplicitShareAcknowledgment(true);
		assertThat(containerProperties.getShareAckMode()).isEqualTo(ShareAckMode.MANUAL);
		assertThat(containerProperties.isExplicitShareAcknowledgment()).isTrue();

		containerProperties.setExplicitShareAcknowledgment(false);
		assertThat(containerProperties.getShareAckMode()).isEqualTo(ShareAckMode.EXPLICIT); // false maps to EXPLICIT (container-managed), not IMPLICIT
		assertThat(containerProperties.isExplicitShareAcknowledgment()).isFalse();
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
	void shouldSupportDefaultBeanNameSetting() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getBeanName()).isEqualTo("noBeanNameSet");
		assertThat(container.getListenerId()).isEqualTo("noBeanNameSet");
	}

	@Test
	void shouldReportRunningStateBeforeStart() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.isRunning()).isFalse();
	}

	@Test
	void shouldUseRejectingRecovererByDefault() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(ShareConsumerRecordRecoverer.REJECTING);
	}

	@Test
	void shouldAllowCustomShareConsumerRecordRecoverer() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		ShareConsumerRecordRecoverer customRecoverer = (record, ex) -> AcknowledgeType.RELEASE;
		container.setShareConsumerRecordRecoverer(customRecoverer);

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(customRecoverer);
	}

	@Test
	void factoryShouldApplyShareConsumerRecordRecovererToContainer() {
		ShareConsumerRecordRecoverer customRecoverer = (record, ex) -> AcknowledgeType.RELEASE;
		ShareKafkaListenerContainerFactory<String, String> factory =
				new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		factory.setShareConsumerRecordRecoverer(customRecoverer);

		KafkaListenerEndpoint endpoint = mock(KafkaListenerEndpoint.class);
		given(endpoint.getTopics()).willReturn(java.util.List.of("test-topic"));
		given(endpoint.getConcurrency()).willReturn(null);

		ShareKafkaMessageListenerContainer<String, String> container =
				factory.createListenerContainer(endpoint);

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(customRecoverer);
	}

	/**
	 * Verifies that even when the factory has share.acknowledgement.mode=implicit in its
	 * base configuration, the container passes share.acknowledgement.mode=explicit as an
	 * override when creating the consumer. This is the critical contract: the container
	 * must enforce explicit mode regardless of what the factory is configured with.
	 */
	@Test
	@SuppressWarnings("unchecked")
	void shouldPassExplicitOverrideToConsumerEvenWhenFactoryConfigIsImplicit() {
		ShareConsumerFactory<String, String> conflictingFactory = mock(ShareConsumerFactory.class);
		given(conflictingFactory.getConfigurationProperties())
				.willReturn(Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "implicit"));

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(conflictingFactory.createShareConsumer(any(), any(), any()))
				.willReturn(mockConsumer);
		lenient().when(mockConsumer.poll(any())).thenReturn(null);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(conflictingFactory, containerProperties);
		container.setBeanName("conflictWarningContainer");
		container.start();
		container.stop();

		verify(conflictingFactory).createShareConsumer(
				any(),
				any(),
				eq(Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit")));
	}

}
