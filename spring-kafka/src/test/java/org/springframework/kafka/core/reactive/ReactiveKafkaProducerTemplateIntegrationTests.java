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

package org.springframework.kafka.core.reactive;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.reactivestreams.Subscription;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

/**
 * @author Mark Norkin
 */
public class ReactiveKafkaProducerTemplateIntegrationTests {
	private static final int DEFAULT_PARTITIONS_COUNT = 2;
	private static final int DEFAULT_KEY = 42;
	private static final String DEFAULT_VALUE = "foo_data";
	private static final int DEFAULT_PARTITION = 1;
	private static final long DEFAULT_TIMESTAMP = Instant.now().toEpochMilli();
	private static final String REACTIVE_INT_KEY_TOPIC = "reactive_int_key_topic";
	private static final Duration DEFAULT_VERIFY_TIMEOUT = Duration.ofSeconds(10);

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, DEFAULT_PARTITIONS_COUNT, REACTIVE_INT_KEY_TOPIC);

	private static ReactiveKafkaConsumerTemplate<Integer, String> reactiveKafkaConsumerTemplate;
	private ReactiveKafkaProducerTemplate<Integer, String> reactiveKafkaProducerTemplate;

	@BeforeClass
	public static void setUpBeforeClass() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("reactive_consumer_group", "false", embeddedKafka);
		reactiveKafkaConsumerTemplate = new ReactiveKafkaConsumerTemplate<>(setupReceiverOptionsWithDefaultTopic(consumerProps));
	}

	@Before
	public void setUp() {
		reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate<>(setupSenderOptionsWithDefaultTopic(), new MessagingMessageConverter());
	}

	private SenderOptions<Integer, String> setupSenderOptionsWithDefaultTopic() {
		Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
		return SenderOptions.create(senderProps);
	}

	private static ReceiverOptions<Integer, String> setupReceiverOptionsWithDefaultTopic(Map<String, Object> consumerProps) {
		ReceiverOptions<Integer, String> basicReceiverOptions = ReceiverOptions.create(consumerProps);
		return basicReceiverOptions
				.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
				.addAssignListener(p -> Assertions.assertThat(p.iterator().next().topicPartition().topic()).isEqualTo(REACTIVE_INT_KEY_TOPIC))
				.subscription(Collections.singletonList(REACTIVE_INT_KEY_TOPIC));
	}

	@After
	public void tearDown() throws Exception {
		reactiveKafkaProducerTemplate.close();
	}

	@Test
	public void shouldSendSingleRecordAsKeyAndReceiveIt() {
		Mono<SenderResult<Void>> senderResultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_VALUE);

		StepVerifier.create(senderResultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
						.extracting(RecordMetadata::topic)
						.containsExactly(REACTIVE_INT_KEY_TOPIC);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE))
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendSingleRecordAsKeyValueAndReceiveIt() {
		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_KEY, DEFAULT_VALUE);

		StepVerifier.create(resultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
						.extracting(RecordMetadata::topic)
						.containsExactly(REACTIVE_INT_KEY_TOPIC);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);
		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendSingleRecordAsPartitionKeyValueAndReceiveIt() {
		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_KEY, DEFAULT_VALUE);

		StepVerifier.create(resultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
						.extracting(RecordMetadata::topic, RecordMetadata::partition)
						.containsExactly(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
					Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendSingleRecordAsPartitionTimestampKeyValueAndReceiveIt() {
		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE);

		StepVerifier.create(resultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
						.extracting(RecordMetadata::topic, RecordMetadata::partition, RecordMetadata::timestamp)
						.containsExactly(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
					Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
					Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendSingleRecordAsProducerRecordAndReceiveIt() {
		List<Header> producerRecordHeaders = convertToKafkaHeaders(
				new SimpleImmutableEntry<>(KafkaHeaders.PARTITION_ID, 0),
				new SimpleImmutableEntry<>("foo", "bar"),
				new SimpleImmutableEntry<>(KafkaHeaders.RECEIVED_TOPIC, "dummy"));

		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE, producerRecordHeaders);

		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(producerRecord);

		StepVerifier.create(resultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
						.extracting(RecordMetadata::topic, RecordMetadata::partition, RecordMetadata::timestamp)
						.containsExactly(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
					Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
					Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
					Assertions.assertThat(receiverRecord.headers().toArray()).isEqualTo(producerRecordHeaders.toArray());
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendSingleRecordAsSenderRecordAndReceiveIt() {
		List<Header> producerRecordHeaders = convertToKafkaHeaders(
				new SimpleImmutableEntry<>(KafkaHeaders.PARTITION_ID, 0),
				new SimpleImmutableEntry<>("foo", "bar"),
				new SimpleImmutableEntry<>(KafkaHeaders.RECEIVED_TOPIC, "dummy"));

		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE, producerRecordHeaders);

		int correlationMetadata = 42;
		SenderRecord<Integer, String, Integer> senderRecord = SenderRecord.create(producerRecord, correlationMetadata);
		Mono<SenderResult<Integer>> resultMono = reactiveKafkaProducerTemplate.send(senderRecord);

		StepVerifier.create(resultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderRecord.correlationMetadata()).isEqualTo(correlationMetadata);
					Assertions.assertThat(senderResult.recordMetadata())
							.extracting(RecordMetadata::topic, RecordMetadata::partition, RecordMetadata::timestamp)
							.containsExactly(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
					Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
					Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
					Assertions.assertThat(receiverRecord.headers().toArray()).isEqualTo(producerRecordHeaders.toArray());
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendSingleRecordAsMessageAndReceiveIt() {
		Message<String> message = MessageBuilder.withPayload(DEFAULT_VALUE)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader("foo", "bar")
				.setHeader(KafkaHeaders.RECEIVED_TOPIC, "dummy")
				.build();

		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, message);

		StepVerifier.create(resultMono)
				.assertNext(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
						.extracting(RecordMetadata::topic)
						.containsExactly(REACTIVE_INT_KEY_TOPIC);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);

					List<Header> messageHeaders = convertToKafkaHeaders(message.getHeaders());
					Assertions.assertThat(receiverRecord.headers().toArray()).isEqualTo(messageHeaders.toArray());
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void sendMultipleRecordsAsPublisherAndReceiveThem() {
		int msgCount = 10;
		List<SenderRecord<Integer, String, Integer>> senderRecords =
				IntStream.range(0, msgCount)
						.mapToObj(i -> SenderRecord.create(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, System.currentTimeMillis(), DEFAULT_KEY, DEFAULT_VALUE + i, i))
						.collect(Collectors.toList());

		Flux<SenderRecord<Integer, String, Integer>> senderRecordWithDelay = Flux.fromIterable(senderRecords).delayElements(Duration.ofMillis(100));
		Flux<SenderResult<Integer>> resultFlux = reactiveKafkaProducerTemplate.send(senderRecordWithDelay);

		StepVerifier.create(resultFlux)
				.recordWith(ArrayList::new)
				.expectNextCount(msgCount)
				.consumeRecordedWith(senderResults -> {
					Assertions.assertThat(senderResults).hasSize(msgCount);

					List<RecordMetadata> records = senderResults.stream().map(SenderResult::recordMetadata).collect(Collectors.toList());

					Assertions.assertThat(records).extracting(RecordMetadata::topic).allSatisfy(actualTopic -> Assertions.assertThat(actualTopic).isEqualTo(REACTIVE_INT_KEY_TOPIC));
					Assertions.assertThat(records).extracting(RecordMetadata::partition).allSatisfy(actualPartition -> Assertions.assertThat(actualPartition).isEqualTo(DEFAULT_PARTITION));
					List<Long> senderRecordsTimestamps = senderRecords.stream().map(SenderRecord::timestamp).collect(Collectors.toList());
					Assertions.assertThat(records).extracting(RecordMetadata::timestamp).containsExactlyElementsOf(senderRecordsTimestamps);
					List<Integer> senderRecordsCorrelationMetadata = senderRecords.stream().map(SenderRecord::correlationMetadata).collect(Collectors.toList());
					Assertions.assertThat(senderRecords).extracting(SenderRecord::correlationMetadata).containsExactlyElementsOf(senderRecordsCorrelationMetadata);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()).take(msgCount))
				.recordWith(ArrayList::new)
				.expectNextCount(msgCount)
				.consumeSubscriptionWith(Subscription::cancel)
				.consumeRecordedWith(receiverRecords -> {
					Assertions.assertThat(receiverRecords).hasSize(msgCount);

					Assertions.assertThat(receiverRecords).extracting(ReceiverRecord::partition).allSatisfy(actualPartition -> Assertions.assertThat(actualPartition).isEqualTo(DEFAULT_PARTITION));
					Assertions.assertThat(receiverRecords).extracting(ReceiverRecord::key).allSatisfy(actualKey -> Assertions.assertThat(actualKey).isEqualTo(DEFAULT_KEY));
					List<Long> senderRecordsTimestamps = senderRecords.stream().map(SenderRecord::timestamp).collect(Collectors.toList());
					Assertions.assertThat(receiverRecords).extracting(ReceiverRecord::timestamp).containsExactlyElementsOf(senderRecordsTimestamps);
					List<String> senderRecordsValues = senderRecords.stream().map(SenderRecord::value).collect(Collectors.toList());
					Assertions.assertThat(receiverRecords).extracting(ReceiverRecord::value).containsExactlyElementsOf(senderRecordsValues);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldFlushRecordsOnDemand() {
		Mono<Void> sendWithFlushMono = reactiveKafkaProducerTemplate
				.send(Mono.just(SenderRecord.create(new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_KEY, DEFAULT_VALUE), null)))
				.then(reactiveKafkaProducerTemplate.flush())
				.then();

		StepVerifier.create(sendWithFlushMono)
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldReturnPartitionsForTopic() {
		Flux<PartitionInfo> topicPartitionsMono = reactiveKafkaProducerTemplate.partitionsFromProducerFor(REACTIVE_INT_KEY_TOPIC);

		StepVerifier.create(topicPartitionsMono)
				.expectNextCount(DEFAULT_PARTITIONS_COUNT)
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldReturnMetrics() {
		Mono<? extends Map<MetricName, ? extends Metric>> metricsMono = reactiveKafkaProducerTemplate.metricsFromProducer();

		StepVerifier.create(metricsMono)
				.assertNext(metrics -> Assertions.assertThat(metrics).isNotNull().isNotEmpty())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@SafeVarargs
	private final List<Header> convertToKafkaHeaders(Map.Entry<String, Object>... headerEntries) {
		Map<String, Object> headers = Stream.of(headerEntries).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		return convertToKafkaHeaders(headers);
	}

	private List<Header> convertToKafkaHeaders(Map<String, Object> headers) {
		KafkaHeaderMapper headerMapper = new DefaultKafkaHeaderMapper();
		RecordHeaders result = new RecordHeaders();
		headerMapper.fromHeaders(new MessageHeaders(headers), result);
		return Stream.of(result.toArray()).collect(Collectors.toList());
	}
}
