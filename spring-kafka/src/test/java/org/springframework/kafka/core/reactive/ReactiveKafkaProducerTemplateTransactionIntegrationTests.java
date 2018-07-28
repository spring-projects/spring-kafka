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

import static org.springframework.kafka.test.assertj.KafkaConditions.match;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscription;

import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

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
public class ReactiveKafkaProducerTemplateTransactionIntegrationTests {
	private static final int DEFAULT_PARTITIONS_COUNT = 2;
	private static final Integer DEFAULT_KEY = 42;
	private static final String DEFAULT_VALUE = "foo_data";
	private static final int DEFAULT_PARTITION = 1;
	private static final long DEFAULT_TIMESTAMP = Instant.now().toEpochMilli();
	private static final String REACTIVE_INT_KEY_TOPIC = "reactive_int_key_topic";
	public static final Duration DEFAULT_VERIFY_TIMEOUT = Duration.ofSeconds(10);

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(3, true, DEFAULT_PARTITIONS_COUNT, REACTIVE_INT_KEY_TOPIC);
	private static Map<String, Object> consumerProps;

	private ReactiveKafkaProducerTemplate<Integer, String> reactiveKafkaProducerTemplate;
	private ReactiveKafkaConsumerTemplate<Integer, String> reactiveKafkaConsumerTemplate;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		consumerProps = KafkaTestUtils.consumerProps("reactive_transaction_consumer_group", "false", embeddedKafka);
	}

	@Before
	public void setUp() {
		Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
		SenderOptions<Integer, String> senderOptions = SenderOptions.create(senderProps);
		senderOptions = senderOptions
				.producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "reactive.transaction")
				.producerProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
				.producerProperty(ProducerConfig.RETRIES_CONFIG, 1);
		RecordMessageConverter messagingConverter = new MessagingMessageConverter();
		reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate<>(senderOptions, messagingConverter);
		reactiveKafkaConsumerTemplate = new ReactiveKafkaConsumerTemplate<>(setupReceiverOptionsWithDefaultTopic(consumerProps));
	}

	private ReceiverOptions<Integer, String> setupReceiverOptionsWithDefaultTopic(Map<String, Object> consumerProps) {
		ReceiverOptions<Integer, String> basicReceiverOptions = ReceiverOptions.create(consumerProps);
		return basicReceiverOptions
				.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
				.consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
				.consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
				.addAssignListener(p -> Assertions.assertThat(p.iterator().next().topicPartition().topic()).isEqualTo(REACTIVE_INT_KEY_TOPIC))
				.subscription(Collections.singletonList(REACTIVE_INT_KEY_TOPIC));
	}

	@After
	public void tearDown() throws Exception {
		reactiveKafkaProducerTemplate.close();
	}

	@Test
	public void shouldSendOneRecordTransactionallyViaTemplateAsSenderRecordAndReceiveIt() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE);
		long correlationMetadata = 1L;
		SenderRecord<Integer, String, Long> senderRecord = SenderRecord.create(producerRecord, correlationMetadata);

		Mono<SenderResult<Long>> publisher = reactiveKafkaProducerTemplate.sendTransactionally(senderRecord);
		StepVerifier.create(publisher)
				.assertNext(senderResult ->
						Assertions.assertThat(senderResult.recordMetadata())
								.has(match(recordMetadata -> REACTIVE_INT_KEY_TOPIC.equals(recordMetadata.topic())))
								.has(match(recordMetadata -> DEFAULT_PARTITION == recordMetadata.partition()))
								.has(match(recordMetadata -> DEFAULT_TIMESTAMP == recordMetadata.timestamp()))
								.has(match(recordMetadata -> correlationMetadata == senderRecord.correlationMetadata()))
				)
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
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
	public void shouldSendOneRecordTransactionallyViaTemplateAsPublisherAndReceiveIt() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE);

		Flux<SenderRecord<Integer, String, Long>> senderRecordsGroupTransaction = Flux.just(SenderRecord.create(producerRecord, 1L));
		Flux<Flux<SenderRecord<Integer, String, Long>>> groupTransactions = Flux.just(senderRecordsGroupTransaction);

		StepVerifier.create(reactiveKafkaProducerTemplate.sendTransactionally(groupTransactions).then())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
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
	public void shouldSendOneRecordTransactionallyViaOutboundAndReceiveIt() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE);

		Flux<SenderRecord<Integer, String, Long>> senderRecordsGroupTransaction = Flux.just(SenderRecord.create(producerRecord, 1L));
		Flux<Flux<SenderRecord<Integer, String, Long>>> groupTransactions = Flux.just(senderRecordsGroupTransaction);

		StepVerifier.create(reactiveKafkaProducerTemplate.createOutbound().sendTransactionally(groupTransactions).then())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive())
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
	public void shouldSendMultipleRecordsTransactionallyViaOutboundAndReceiveIt() {
		int recordsCountInGroup = 10;
		int transactionGroupsCount = 3;
		int expectedTotalRecordsCount = recordsCountInGroup * transactionGroupsCount;

		Flux<Flux<SenderRecord<Integer, String, Integer>>> groupTransactions =
				Flux.range(1, transactionGroupsCount)
						.map(transactionGroupNumber -> generateSenderRecords(recordsCountInGroup, transactionGroupNumber));

		StepVerifier.create(reactiveKafkaProducerTemplate.createOutbound().sendTransactionally(groupTransactions).then())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		Flux<ReceiverRecord<Integer, String>> receiverRecordFlux = reactiveKafkaConsumerTemplate.receive()
				.filter(rr -> !DEFAULT_VALUE.equals(rr.value())); // skip records published from other tests

		StepVerifier.create(receiverRecordFlux)
				.recordWith(ArrayList::new)
				.expectNextCount(expectedTotalRecordsCount)
				.consumeSubscriptionWith(Subscription::cancel)
				.consumeRecordedWith(receiverRecords -> {
					Assertions.assertThat(receiverRecords).hasSize(expectedTotalRecordsCount);

					//check first record value
					ReceiverRecord<Integer, String> firstRecord = receiverRecords.iterator().next();
					Assertions.assertThat(firstRecord.value()).endsWith(String.valueOf(10));

					receiverRecords.forEach(receiverRecord -> {
						Assertions.assertThat(receiverRecord.partition()).isEqualTo(0);
						Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
						Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
						Assertions.assertThat(receiverRecord.value()).startsWith(DEFAULT_VALUE);
					});

					//check last record value
					Optional<ReceiverRecord<Integer, String>> lastRecord = receiverRecords.stream().skip(expectedTotalRecordsCount - 1).findFirst();
					Assertions.assertThat(lastRecord.isPresent()).isEqualTo(true);
					lastRecord.ifPresent(last -> Assertions.assertThat(last.value()).endsWith(String.valueOf(recordsCountInGroup * (int) Math.pow(10, transactionGroupsCount))));
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendMultipleRecordsTransactionallyViaTemplateAndReceiveIt() {
		int recordsCountInGroup = 10;
		int transactionGroupsCount = 3;
		int expectedTotalRecordsCount = recordsCountInGroup * transactionGroupsCount;

		Flux<Flux<SenderRecord<Integer, String, Integer>>> groupTransactions =
				Flux.range(1, transactionGroupsCount)
						.map(transactionGroupNumber -> generateSenderRecords(recordsCountInGroup, transactionGroupNumber));

		StepVerifier.create(reactiveKafkaProducerTemplate.sendTransactionally(groupTransactions).then())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		Flux<ReceiverRecord<Integer, String>> receiverRecordFlux = reactiveKafkaConsumerTemplate.receive()
				.filter(rr -> !DEFAULT_VALUE.equals(rr.value())); // skip records published from other tests

		StepVerifier.create(receiverRecordFlux)
				.recordWith(ArrayList::new)
				.expectNextCount(expectedTotalRecordsCount)
				.consumeSubscriptionWith(Subscription::cancel)
				.consumeRecordedWith(receiverRecords -> {
					Assertions.assertThat(receiverRecords).hasSize(expectedTotalRecordsCount);

					//check first record value
					ReceiverRecord<Integer, String> firstRecord = receiverRecords.iterator().next();
					Assertions.assertThat(firstRecord.value()).endsWith(String.valueOf(10));

					receiverRecords.forEach(receiverRecord -> {
						Assertions.assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
						Assertions.assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
						Assertions.assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
						Assertions.assertThat(receiverRecord.value()).startsWith(DEFAULT_VALUE);
					});

					//check last record value
					Optional<ReceiverRecord<Integer, String>> lastRecord = receiverRecords.stream().skip(expectedTotalRecordsCount - 1).findFirst();
					Assertions.assertThat(lastRecord.isPresent()).isEqualTo(true);
					lastRecord.ifPresent(last -> Assertions.assertThat(last.value()).endsWith(String.valueOf(recordsCountInGroup * (int) Math.pow(10, transactionGroupsCount))));
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	private Flux<SenderRecord<Integer, String, Integer>> generateSenderRecords(int recordsCount, int seed) {
		return Flux.range(1, recordsCount)
				.map(i -> {
					int correlationMetadata = i * (int) Math.pow(10, seed);
					return SenderRecord.create(new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY, DEFAULT_VALUE + correlationMetadata), correlationMetadata);
				});
	}

	@Test//todo
	@Ignore
	public void shouldSendOffsetsToTransaction() {
		Map<TopicPartition, OffsetAndMetadata> offsets = null;
		String consumerId = null;
		Mono<Void> sendOffsetsToTransaction = reactiveKafkaProducerTemplate.sendOffsetsToTransaction(offsets, consumerId);
	}
}
