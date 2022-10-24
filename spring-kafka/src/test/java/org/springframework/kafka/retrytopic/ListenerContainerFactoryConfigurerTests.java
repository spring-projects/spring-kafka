/*
 * Copyright 2018-2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
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
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
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
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
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
	private ArgumentCaptor<DefaultErrorHandler> errorHandlerCaptor;

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
	private java.util.function.Consumer<DefaultErrorHandler> errorHandlerCustomizer;

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

	@Mock
	private KafkaListenerEndpoint endpoint;

	private final long backOffValue = 2000L;

	private final ListenerContainerFactoryConfigurer.Configuration lcfcConfiguration =
			new ListenerContainerFactoryConfigurer.Configuration(Collections.singletonList(backOffValue));

	@SuppressWarnings("deprecation")
	@Test
	void shouldSetupErrorHandling() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getAckMode()).willReturn(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		given(containerProperties.getCommitCallback()).willReturn(offsetCommitCallback);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);
		willReturn(container).given(containerFactory).createListenerContainer(endpoint);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setErrorHandlerCustomizer(errorHandlerCustomizer);
		KafkaListenerContainerFactory<?> factory = configurer.decorateFactory(containerFactory,
				configuration.forContainerFactoryConfigurer());
		factory.createListenerContainer(endpoint);

		// then
		then(container).should(times(1)).setCommonErrorHandler(errorHandlerCaptor.capture());
		DefaultErrorHandler errorHandler = errorHandlerCaptor.getValue();

		RuntimeException ex = new RuntimeException();
		errorHandler.handleRemaining(ex, records, consumer, container);

		then(recoverer).should(times(1)).accept(record, consumer, ex);
		then(consumer).should(times(1)).commitAsync(any(Map.class), eq(offsetCommitCallback));
		then(errorHandlerCustomizer).should(times(1)).accept(errorHandler);

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
		willReturn(container).given(containerFactory).createListenerContainer(endpoint);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setContainerCustomizer(configurerContainerCustomizer);
		KafkaListenerContainerFactory<?> factory = configurer
				.decorateFactory(containerFactory, configuration.forContainerFactoryConfigurer());
		factory.createListenerContainer(endpoint);

		// then
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
	void shouldDecorateFactory() {

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
		willReturn(container).given(containerFactory).createListenerContainer(endpoint);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setContainerCustomizer(configurerContainerCustomizer);
		KafkaListenerContainerFactory<?> factory = configurer
				.decorateFactory(containerFactory, configuration.forContainerFactoryConfigurer());
		factory.createListenerContainer(endpoint);

		// then
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
	void shouldUseGivenBackOffAndExceptions() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);
		willReturn(container).given(containerFactory).createListenerContainer(endpoint);
		BackOff backOffMock = mock(BackOff.class);
		BackOffExecution backOffExecutionMock = mock(BackOffExecution.class);
		given(backOffMock.start()).willReturn(backOffExecutionMock);

		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setBlockingRetriesBackOff(backOffMock);
		configurer.setBlockingRetryableExceptions(IllegalArgumentException.class, IllegalStateException.class);

		// when
		KafkaListenerContainerFactory<?> decoratedFactory =
				configurer.decorateFactory(this.containerFactory, configuration.forContainerFactoryConfigurer());
		decoratedFactory.createListenerContainer(endpoint);

		// then
		then(backOffMock).should().start();
		then(container).should().setCommonErrorHandler(errorHandlerCaptor.capture());
		CommonErrorHandler errorHandler = errorHandlerCaptor.getValue();
		assertThat(DefaultErrorHandler.class.isAssignableFrom(errorHandler.getClass())).isTrue();
		DefaultErrorHandler defaultErrorHandler = (DefaultErrorHandler) errorHandler;
		assertThat(defaultErrorHandler.removeClassification(IllegalArgumentException.class)).isTrue();
		assertThat(defaultErrorHandler.removeClassification(IllegalStateException.class)).isTrue();
		assertThat(defaultErrorHandler.removeClassification(ConversionException.class)).isNull();

	}

	@Test
	void shouldUseGivenBackOffAndExceptionsKeepStandard() {

		// given
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create()).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(listener);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);
		willReturn(container).given(containerFactory).createListenerContainer(endpoint);
		BackOff backOffMock = mock(BackOff.class);
		BackOffExecution backOffExecutionMock = mock(BackOffExecution.class);
		given(backOffMock.start()).willReturn(backOffExecutionMock);

		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setBlockingRetriesBackOff(backOffMock);
		configurer.setBlockingRetryableExceptions(IllegalArgumentException.class, IllegalStateException.class);
		configurer.setRetainStandardFatal(true);

		// when
		KafkaListenerContainerFactory<?> decoratedFactory =
				configurer.decorateFactory(this.containerFactory, configuration.forContainerFactoryConfigurer());
		decoratedFactory.createListenerContainer(endpoint);

		// then
		then(backOffMock).should().start();
		then(container).should().setCommonErrorHandler(errorHandlerCaptor.capture());
		CommonErrorHandler errorHandler = errorHandlerCaptor.getValue();
		assertThat(DefaultErrorHandler.class.isAssignableFrom(errorHandler.getClass())).isTrue();
		DefaultErrorHandler defaultErrorHandler = (DefaultErrorHandler) errorHandler;
		assertThat(defaultErrorHandler.removeClassification(IllegalArgumentException.class)).isTrue();
		assertThat(defaultErrorHandler.removeClassification(IllegalStateException.class)).isTrue();
		assertThat(defaultErrorHandler.removeClassification(ConversionException.class)).isFalse();

	}

	@Test
	void shouldThrowIfBackOffOrRetryablesAlreadySet() {
		// given
		BackOff backOff = new FixedBackOff();
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						deadLetterPublishingRecovererFactory, clock);
		configurer.setBlockingRetriesBackOff(backOff);
		configurer.setBlockingRetryableExceptions(IllegalArgumentException.class, IllegalStateException.class);

		// when / then
		assertThatThrownBy(() -> configurer.setBlockingRetriesBackOff(backOff)).isInstanceOf(IllegalStateException.class);
		assertThatThrownBy(() -> configurer.setBlockingRetryableExceptions(ConversionException.class, DeserializationException.class))
				.isInstanceOf(IllegalStateException.class);
	}

}
