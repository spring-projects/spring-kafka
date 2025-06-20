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

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;


/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class KafkaBackoffAwareMessageListenerAdapterTests {

	@Mock
	private AcknowledgingConsumerAwareMessageListener<Object, Object> delegate;

	@Mock
	private Acknowledgment ack;

	@Mock
	private ConsumerRecord<Object, Object> data;

	@Mock
	private Consumer<?, ?> consumer;

	@Mock
	private Headers headers;

	@Mock
	private Header header;

	@Captor
	private ArgumentCaptor<Long> timestampCaptor;

	@Mock
	private KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	private final KafkaConsumerBackoffManager.Context context = mock(KafkaConsumerBackoffManager.Context.class);

	private final Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	private final long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private final byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	private final String testTopic = "testTopic";

	private final int testPartition = 0;

	private final TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

	private final String listenerId = "testListenerId";

	@Test
	void shouldJustDelegateIfNoBackoffHeaderPresent() {

		// setup
		given(data.headers()).willReturn(headers);
		given(headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP)).willReturn(null);

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId, clock);

		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		then(delegate)
				.should(times(1))
				.onMessage(data, ack, consumer);
	}

	private void setupCallBackOffManager() {
		given(data.headers()).willReturn(headers);
		given(headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP))
				.willReturn(header);

		given(header.value())
				.willReturn(originalTimestampBytes);
		given(data.topic()).willReturn(testTopic);
		given(data.partition()).willReturn(testPartition);
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndFirstMethodIsCalled() {

		// given
		setupCallBackOffManager();

		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition, null))
				.willReturn(context);

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId, clock);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition), isNull());
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.backOffIfNecessary(context);

		then(delegate).should(times(1)).onMessage(eq(data), any(), isNull());
	}

	@Test
	void shouldWrapExceptionInTimestampedException() {

		// given
		setupCallBackOffManager();

		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition, null))
				.willReturn(context);
		RuntimeException thrownException = new RuntimeException();
		willThrow(thrownException).given(delegate).onMessage(eq(data), any(), isNull());

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId, clock);

		// when
		assertThatThrownBy(() -> backoffAwareMessageListenerAdapter.onMessage(data))
				.isExactlyInstanceOf(TimestampedException.class).cause().isEqualTo(thrownException);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition), isNull());
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.backOffIfNecessary(context);

		then(delegate).should(times(1)).onMessage(eq(data), any(), isNull());
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndSecondMethodIsCalled() {

		// given
		setupCallBackOffManager();

		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition, null))
				.willReturn(context);

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId, clock);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data, ack);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition), isNull());
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.backOffIfNecessary(context);

		then(delegate).should(times(1)).onMessage(data, ack, null);
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndThirdMethodIsCalled() {

		// given
		setupCallBackOffManager();
		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition, consumer))
				.willReturn(context);

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId, clock);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data, consumer);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition), eq(consumer));
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.backOffIfNecessary(context);

		then(delegate).should(times(1)).onMessage(eq(data), any(), eq(consumer));
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndFourthMethodIsCalled() {

		// setup
		setupCallBackOffManager();
		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition, consumer))
				.willReturn(context);


		// given
		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId, clock);

		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition), eq(consumer));
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.backOffIfNecessary(context);

		then(delegate).should(times(1)).onMessage(data, ack, consumer);
	}
}
