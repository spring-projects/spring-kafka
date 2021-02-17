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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.times;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class DeadLetterPublishingRecovererFactoryTests {

	private final Clock clock = TestClockUtils.CLOCK;

	@Mock
	private DestinationTopicResolver destinationTopicResolver;

	private String testTopic = "test-topic";

	private String testRetryTopic = "test-topic-retry-0";

	private final Object key = new Object();

	private final Object value = new Object();

	private ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>(testTopic, 2, 0, key, value);

	@Mock
	private DestinationTopic destinationTopic;

	@Mock
	private KafkaOperations<?, ?> kafkaOperations;

	@Mock
	private KafkaOperations<?, ?> kafkaOperations2;

	@Mock
	private ListenableFuture<?> listenableFuture;

	@Captor
	private ArgumentCaptor<ProducerRecord> producerRecordCaptor;

	@Mock
	private Consumer<DeadLetterPublishingRecoverer> dlprCustomizer;

	private long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	@Test
	void shouldSendMessage() {
		// setup
		RuntimeException e = new RuntimeException();
		given(destinationTopicResolver.resolveNextDestination(testTopic, 1, e, originalTimestamp)).willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopic.getDestinationPartitions()).willReturn(3);
		given(destinationTopicResolver.resolveDestinationNextExecutionTimestamp(testTopic, 1, e, originalTimestamp))
				.willReturn(getNowTimestamp());
		given(destinationTopicResolver.getCurrentTopic(testRetryTopic)).willReturn(destinationTopic);
		willReturn(this.kafkaOperations2).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations2.send(any(ProducerRecord.class))).willReturn(listenableFuture);
		this.consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampBytes);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		then(kafkaOperations2).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		assertEquals(testRetryTopic, producerRecord.topic());
		assertEquals(value, producerRecord.value());
		assertEquals(key, producerRecord.key());
		assertEquals(2, producerRecord.partition());

		// assert headers
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertNotNull(attemptsHeader);
		assertEquals(2, attemptsHeader.value()[0]);
		Header timestampHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP);
		assertNotNull(timestampHeader);
		assertEquals(getNowTimestamp(), new BigInteger(timestampHeader.value()).longValue());
	}

	@Test
	void shouldIncreaseAttempts() {

		// setup
		RuntimeException e = new RuntimeException();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, key, value);
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, BigInteger.valueOf(1).toByteArray());
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, this.originalTimestampBytes);

		given(destinationTopicResolver.resolveNextDestination(testTopic, 1, e, originalTimestamp)).willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopic.getDestinationPartitions()).willReturn(1);
		given(destinationTopicResolver.resolveDestinationNextExecutionTimestamp(testTopic, 1, e, this.originalTimestamp))
				.willReturn(getNowTimestamp());
		given(destinationTopicResolver.getCurrentTopic(testRetryTopic)).willReturn(destinationTopic);
		willReturn(this.kafkaOperations2).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations2.send(any(ProducerRecord.class))).willReturn(listenableFuture);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations2).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertNotNull(attemptsHeader);
		assertEquals(2, attemptsHeader.value()[0]);
	}

	@Test
	void shouldAddOriginalTimestampHeader() {

		// setup
		RuntimeException e = new RuntimeException();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, originalTimestamp,
				TimestampType.CREATE_TIME, 1234L, -1, -1, key, value);

		given(destinationTopicResolver.resolveNextDestination(testTopic, 1, e, originalTimestamp)).willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopic.getDestinationPartitions()).willReturn(1);
		given(destinationTopicResolver.getCurrentTopic(testRetryTopic)).willReturn(destinationTopic);
		long nextExecutionTimestamp = getNowTimestamp() + destinationTopic.getDestinationDelay();
		given(destinationTopicResolver.resolveDestinationNextExecutionTimestamp(testTopic, 1, e, this.originalTimestamp))
				.willReturn(nextExecutionTimestamp);
		willReturn(this.kafkaOperations2).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations2.send(any(ProducerRecord.class))).willReturn(listenableFuture);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations2).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header originalTimestampHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
		assertNotNull(originalTimestampHeader);
		assertEquals(getNowTimestamp(),	new BigInteger(originalTimestampHeader.value()).longValue());
	}

	@Test
	void shouldNotReplaceOriginalTimestampHeader() {

		// setup
		RuntimeException e = new RuntimeException();
		long timestamp = LocalDateTime.now(this.clock).toInstant(ZoneOffset.UTC).minusMillis(5000).toEpochMilli();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, timestamp,
				TimestampType.CREATE_TIME, 1234L, -1, -1, key, value);

		given(destinationTopicResolver.resolveNextDestination(testTopic, 1, e, timestamp)).willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopic.getDestinationPartitions()).willReturn(1);
		given(destinationTopicResolver.getCurrentTopic(testRetryTopic)).willReturn(destinationTopic);
		long nextExecutionTimestamp = getNowTimestamp() + destinationTopic.getDestinationDelay();
		given(destinationTopicResolver.resolveDestinationNextExecutionTimestamp(testTopic, 1, e, timestamp))
				.willReturn(nextExecutionTimestamp);
		willReturn(this.kafkaOperations2).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations2.send(any(ProducerRecord.class))).willReturn(listenableFuture);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations2).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header originalTimestampHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
		assertNotNull(originalTimestampHeader);
		assertEquals(timestamp,	new BigInteger(originalTimestampHeader.value()).longValue());
	}

	@Test
	void shouldNotSendMessageIfNoOpsDestination() {
		// setup
		RuntimeException e = new RuntimeException();
		given(destinationTopicResolver.resolveNextDestination(testTopic, 1, e, originalTimestamp)).willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(true);
		given(destinationTopicResolver.resolveDestinationNextExecutionTimestamp(testTopic, 1, e, originalTimestamp))
				.willReturn(getNowTimestamp());
		this.consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampBytes);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		then(kafkaOperations2).should(times(0)).send(any(ProducerRecord.class));
	}

	@Test
	void shouldThrowIfKafkaBackoffException() {
		// setup
		RuntimeException e = new KafkaBackoffException("KBEx", null, "test-listener-id", getNowTimestamp());

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		assertThrows(NestedRuntimeException.class, () -> deadLetterPublishingRecoverer.accept(this.consumerRecord, e));

		// then
		then(kafkaOperations2).should(times(0)).send(any(ProducerRecord.class));
	}

	@Test
	void shouldCallDLPRCustomizer() {

		// setup
		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);
		factory.setDeadLetterPublishingRecovererCustomizer(dlprCustomizer);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);

		// then
		then(dlprCustomizer).should(times(1)).accept(deadLetterPublishingRecoverer);
	}


	@Test
	void shouldThrowIfNoTemplateProvided() {
		assertThrows(IllegalArgumentException.class, () -> new DeadLetterPublishingRecovererFactory.Configuration(null));
	}

	private long getNowTimestamp() {
		return Instant.now(this.clock).toEpochMilli();
	}
}
