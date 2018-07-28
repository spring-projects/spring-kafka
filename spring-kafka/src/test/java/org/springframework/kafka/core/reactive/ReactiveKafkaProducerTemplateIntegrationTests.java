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

import static org.springframework.kafka.test.assertj.KafkaConditions.matchingCondition;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

/**
 * @author Mark Norkin
 */
public class ReactiveKafkaProducerTemplateIntegrationTests {
	private static final int DEFAULT_PARTITIONS_COUNT = 2;
	private static final Integer DEFAULT_KEY = 42;
	private static final String DEFAULT_VALUE = "foo_data";
	private static final int DEFAULT_PARTITION = 1;
	private static final long DEFAULT_TIMESTAMP = Instant.now().toEpochMilli();
	private static final String REACTIVE_INT_KEY_TOPIC = "reactive_int_key_topic";
	private static final String REACTIVE_STRING_KEY_TOPIC = "reactive_string_key_topic";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, DEFAULT_PARTITIONS_COUNT, REACTIVE_INT_KEY_TOPIC, REACTIVE_STRING_KEY_TOPIC);
	private static Map<String, Object> consumerProps;

	private ReactiveKafkaProducerTemplate<Integer, String> reactiveKafkaProducerTemplate;
	private ReactiveKafkaConsumerTemplate<Integer, String> reactiveKafkaConsumerTemplate;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		consumerProps = KafkaTestUtils.consumerProps("reactive_consumer_group", "true", embeddedKafka);
	}

	@Before
	public void setUp() {
		Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

		SenderOptions<Integer, String> senderOptions = SenderOptions.create(senderProps);
		RecordMessageConverter messagingConverter = new MessagingMessageConverter();
		reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate<>(senderOptions, false, messagingConverter);
		reactiveKafkaConsumerTemplate = new ReactiveKafkaConsumerTemplate<>(setupReceiverOptionsWithDefaultTopic(consumerProps));
	}

	private ReceiverOptions<Integer, String> setupReceiverOptionsWithDefaultTopic(Map<String, Object> consumerProps) {
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
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));
					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
	}

	@Test
	public void shouldSendSingleRecordAsKeyValueAndReceiveIt() {
		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_KEY, DEFAULT_VALUE);

		StepVerifier.create(resultMono)
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));
					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
	}

	@Test
	public void shouldSendSingleRecordAsPartitionKeyValueAndReceiveIt() {
		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_KEY, DEFAULT_VALUE);

		StepVerifier.create(resultMono)
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())))
							.has(matchingCondition(recordMetadata -> DEFAULT_PARTITION == recordMetadata.partition()));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
								Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));

					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
	}

	@Test
	public void shouldSendSingleRecordAsPartitionTimestampKeyValueAndReceiveIt() {
		Mono<SenderResult<Void>> resultMono = reactiveKafkaProducerTemplate.send(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE);

		StepVerifier.create(resultMono)
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())))
							.has(matchingCondition(recordMetadata -> DEFAULT_PARTITION == recordMetadata.partition()))
							.has(matchingCondition(recordMetadata -> DEFAULT_TIMESTAMP == recordMetadata.timestamp()));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
								Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
								Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);

								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));

					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
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
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())))
							.has(matchingCondition(recordMetadata -> DEFAULT_PARTITION == recordMetadata.partition()))
							.has(matchingCondition(recordMetadata -> DEFAULT_TIMESTAMP == recordMetadata.timestamp()));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
								Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
								Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
								Assertions.assertThat(receiverRecord.headers().toArray()).isEqualTo(producerRecordHeaders.toArray());
								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));

					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
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
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())))
							.has(matchingCondition(recordMetadata -> DEFAULT_PARTITION == recordMetadata.partition()))
							.has(matchingCondition(recordMetadata -> DEFAULT_TIMESTAMP == recordMetadata.timestamp()))
							.has(matchingCondition(recordMetadata -> correlationMetadata == senderRecord.correlationMetadata()));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
								Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
								Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
								Assertions.assertThat(receiverRecord.headers().toArray()).isEqualTo(producerRecordHeaders.toArray());
								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));
					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
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
				.expectNextMatches(senderResult -> {
					Assertions.assertThat(senderResult.recordMetadata())
							.has(matchingCondition(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())));

					StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
							.expectNextMatches(receiverRecord -> {
								Assertions.assertThat(receiverRecord.partition()).isEqualTo(senderResult.recordMetadata().partition());
								Assertions.assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);

								List<Header> messageHeaders = convertToKafkaHeaders(message.getHeaders());
								Assert.assertArrayEquals(messageHeaders.toArray(), receiverRecord.headers().toArray());
								Assertions.assertThat(receiverRecord.headers().toArray()).isEqualTo(messageHeaders.toArray());
								return true;
							})
							.thenCancel()
							.verify(Duration.ofSeconds(10));

					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
	}

	@Test//todo
	@Ignore
	public void sendMultipleRecordsAsPublisherAndReceiveThem() {
	}

	@Test//todo
	@Ignore
	public void sendTransactionally() {
	}

	@Test//todo
	@Ignore
	public void shouldSendOffsetsToTransaction() {
		Map<TopicPartition, OffsetAndMetadata> offsets = null;
		String consumerId = null;
		Mono<Void> sendOffsetsToTransaction = reactiveKafkaProducerTemplate.sendOffsetsToTransaction(offsets, consumerId);
	}

	@Test//todo
	@Ignore
	public void shouldFlushRecordsOnDemand() {
		Mono<Void> flushMono = reactiveKafkaProducerTemplate.flush();
	}

	@Test//todo
	@Ignore
	public void shouldAutoFlushWhenEnabled() {
	}

	@Test
	public void shouldReturnPartitionsForTopic() {
		Mono<List<PartitionInfo>> topicPartitionsMono = reactiveKafkaProducerTemplate.partitionsFromProducerFor(REACTIVE_INT_KEY_TOPIC);

		StepVerifier.create(topicPartitionsMono)
				.expectNextMatches(partitionInfo -> {
					Assertions.assertThat(partitionInfo).isNotNull().hasSize(DEFAULT_PARTITIONS_COUNT);
					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
	}

	@Test
	public void shouldReturnMetrics() {
		Mono<? extends Map<MetricName, ? extends Metric>> metricsMono = reactiveKafkaProducerTemplate.metricsFromProducer();

		StepVerifier.create(metricsMono)
				.expectNextMatches(metrics -> {
					Assertions.assertThat(metrics).isNotNull().isNotEmpty();
					return true;
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
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
