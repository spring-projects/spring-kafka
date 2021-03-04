/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

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
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;


/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class KafkaBackoffAwareMessageListenerAdapterTest {

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

	@Mock
	private KafkaConsumerBackoffManager.Context context;

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
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

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

		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition))
				.willReturn(context);
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndFirstMethodIsCalled() {

		// given
		setupCallBackOffManager();

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition));
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.maybeBackoff(context);

		then(delegate).should(times(1)).onMessage(data, null, null);
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndSecondMethodIsCalled() {

		// given
		setupCallBackOffManager();

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data, ack);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition));
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.maybeBackoff(context);

		then(delegate).should(times(1)).onMessage(data, ack, null);
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndThirdMethodIsCalled() {

		// given
		setupCallBackOffManager();

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data, consumer);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition));
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.maybeBackoff(context);

		then(delegate).should(times(1)).onMessage(data, null, consumer);
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresentAndFourthMethodIsCalled() {

		// setup
		setupCallBackOffManager();


		// given
		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition));
		assertThat(timestampCaptor.getValue()).isEqualTo(originalTimestamp);
		then(kafkaConsumerBackoffManager).should(times(1))
				.maybeBackoff(context);

		then(delegate).should(times(1)).onMessage(data, ack, consumer);
	}

}
