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

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.adapter.AbstractDelegatingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class ListenerContainerFactoryConfigurerTests {

	@Mock
	private KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	@Mock
	private DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	@Mock
	private DeadLetterPublishingRecoverer recoverer;

	@Mock
	private ContainerProperties containerProperties;

	@Captor
	private ArgumentCaptor<CommonErrorHandler> errorHandlerCaptor;

	private final ConsumerRecord<?, ?> record =
			new ConsumerRecord<>("test-topic", 1, 1234L, new Object(), new Object());

	private final List<ConsumerRecord<?, ?>> records = Collections.singletonList(record);

	@Mock
	private Consumer<?, ?> consumer;

	@Mock
	private ConcurrentMessageListenerContainer<?, ?> container;

	@Mock
	private OffsetCommitCallback offsetCommitCallback;

	@Mock
	private java.util.function.Consumer<CommonErrorHandler> errorHandlerCustomizer;

	@SuppressWarnings("rawtypes")
	@Captor
	private ArgumentCaptor<ContainerCustomizer> containerCustomizerCaptor;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory;

	@Mock
	private AcknowledgingConsumerAwareMessageListener<?, ?> listener;

	@Captor
	private ArgumentCaptor<AbstractDelegatingMessageListenerAdapter<?>> listenerAdapterCaptor;

	@SuppressWarnings("rawtypes")
	@Mock
	private ConsumerRecord data;

	@Mock
	private Acknowledgment ack;

	@Captor
	private ArgumentCaptor<String> listenerIdCaptor;

	@Mock
	private java.util.function.Consumer<ConcurrentMessageListenerContainer<?, ?>> configurerContainerCustomizer;

	private final Clock clock = TestClockUtils.CLOCK;

	private final long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private final byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	@Mock
	private RetryTopicConfiguration configuration;

	private final long backOffValue = 2000L;

	private final ListenerContainerFactoryConfigurer.Configuration lcfcConfiguration =
			new ListenerContainerFactoryConfigurer.Configuration(Collections.singletonList(backOffValue));

	@Test
	void shouldSetupErrorHandling() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getAckMode()).willReturn(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		given(containerProperties.getCommitCallback()).willReturn(offsetCommitCallback);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setErrorHandlerCustomizer(errorHandlerCustomizer);
		configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory).should(times(1)).setCommonErrorHandler(errorHandlerCaptor.capture());
		CommonErrorHandler errorHandler = errorHandlerCaptor.getValue();
		assertThat(DefaultErrorHandler.class.isAssignableFrom(errorHandler.getClass())).isTrue();
		DefaultErrorHandler seekToCurrent = (DefaultErrorHandler) errorHandler;

		RuntimeException ex = new RuntimeException();
		seekToCurrent.handleRemaining(ex, records, consumer, container);

		then(recoverer).should(times(1)).accept(record, consumer, ex);
		then(consumer).should(times(1)).commitAsync(any(Map.class), eq(offsetCommitCallback));
		then(errorHandlerCustomizer).should(times(1)).accept(errorHandler);

	}

	@Test
	void shouldSetPartitionEventIntervalAndPollTimout() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);
		given(containerProperties.getPollTimeout()).willReturn(ContainerProperties.DEFAULT_POLL_TIMEOUT);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory).should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(containerProperties).should(times(1))
				.setIdlePartitionEventInterval(backOffValue / 4);
		then(containerProperties).should(times(1))
				.setPollTimeout(backOffValue / 4);
	}

	@Test
	void shouldNotOverridePollTimeoutIfNotDefault() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);
		long previousPollTimoutValue = 3000L;
		given(containerProperties.getPollTimeout()).willReturn(previousPollTimoutValue);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory).should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(containerProperties).should(times(1))
				.setIdlePartitionEventInterval(previousPollTimoutValue);
		then(containerProperties).should(times(1))
				.setPollTimeout(previousPollTimoutValue);
	}

	@Test
	void shouldApplyMinimumPollTimeoutLimit() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer())
				.willReturn(new ListenerContainerFactoryConfigurer.Configuration(Collections.singletonList(500L)));
		given(containerProperties.getPollTimeout()).willReturn(ContainerProperties.DEFAULT_POLL_TIMEOUT);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory).should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(containerProperties).should(times(1))
				.setIdlePartitionEventInterval(100L);
		then(containerProperties).should(times(1))
				.setPollTimeout(100L);
	}

	@Test
	void shouldApplyMaximumPollTimeoutLimit() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer())
				.willReturn(new ListenerContainerFactoryConfigurer.Configuration(Collections.singletonList(30000L)));
		given(containerProperties.getPollTimeout()).willReturn(ContainerProperties.DEFAULT_POLL_TIMEOUT);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory).should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(containerProperties).should(times(1))
				.setIdlePartitionEventInterval(5000L);
		then(containerProperties).should(times(1))
				.setPollTimeout(5000L);
	}

	@Test
	void shouldNotSetPolltimoutAndPartitionIdleIfNoBackOffProvided() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer())
				.willReturn(lcfcConfiguration);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer
				.configureWithoutBackOffValues(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory).should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(containerProperties).should(never())
				.setIdlePartitionEventInterval(anyLong());
		then(containerProperties).should(never())
				.setPollTimeout(anyLong());
	}

	@Test
	void shouldSetupMessageListenerAdapter() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		RecordHeaders headers = new RecordHeaders();
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP, originalTimestampBytes);
		given(data.headers()).willReturn(headers);
		String testListenerId = "testListenerId";
		given(container.getListenerId()).willReturn(testListenerId);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setContainerCustomizer(configurerContainerCustomizer);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		then(containerFactory)
				.should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(container).should(times(1)).setupMessageListener(listenerAdapterCaptor.capture());
		KafkaBackoffAwareMessageListenerAdapter<?, ?> listenerAdapter =
				(KafkaBackoffAwareMessageListenerAdapter<?, ?>) listenerAdapterCaptor.getValue();
		listenerAdapter.onMessage(data, ack, consumer);

		then(this.kafkaConsumerBackoffManager).should(times(1))
				.createContext(anyLong(), listenerIdCaptor.capture(), any(TopicPartition.class), eq(consumer));
		assertThat(listenerIdCaptor.getValue()).isEqualTo(testListenerId);
		then(listener).should(times(1)).onMessage(data, ack, consumer);

		then(this.configurerContainerCustomizer).should(times(1)).accept(container);
	}

	@Test
	void shouldCacheFactoryInstances() {

		// given
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());
		ConcurrentKafkaListenerContainerFactory<?, ?> secondFactory = configurer
				.configure(containerFactory, configuration.forContainerFactoryConfigurer());

		// then
		assertThat(secondFactory).isEqualTo(factory);
		then(containerFactory).should(times(1)).setContainerCustomizer(any());
		then(containerFactory).should(times(1)).setCommonErrorHandler(any());
	}
}
