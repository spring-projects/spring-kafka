/*
 * Copyright 2019-2020 the original author or authors.
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

package org.springframework.kafka.core.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.condition.LogLevels;
import org.springframework.kafka.test.condition.LogLevelsCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.JUnitUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.internals.DefaultKafkaSender;
import reactor.test.StepVerifier;

/**
 * @author Mark Norkin
 * @author Gary Russell
 * @author Will Kennedy
 *
 * @since 2.3.0
 */
@EmbeddedKafka(topics = ReactiveKafkaProducerTemplateTransactionIntegrationTests.REACTIVE_INT_KEY_TOPIC,
		brokerProperties = { "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class ReactiveKafkaProducerTemplateTransactionIntegrationTests {

	private static final LogAccessor logger = new LogAccessor(
			LogFactory.getLog(ReactiveKafkaProducerTemplateTransactionIntegrationTests.class));

	private static final String CONSUMER_GROUP_ID = "reactive_transaction_consumer_group";

	private static final int DEFAULT_KEY = 42;

	private static final String DEFAULT_VALUE = "foo_data";

	private static final int DEFAULT_PARTITION = 1;

	private static final long DEFAULT_TIMESTAMP = Instant.now().toEpochMilli();

	public static final String REACTIVE_INT_KEY_TOPIC = "reactive_int_key_topic";

	private static final Duration DEFAULT_VERIFY_TIMEOUT = Duration.ofSeconds(10);

	private static ReactiveKafkaConsumerTemplate<Integer, String> reactiveKafkaConsumerTemplate;

	private ReactiveKafkaProducerTemplate<Integer, String> reactiveKafkaProducerTemplate;

	@BeforeAll
	public static void setUpBeforeClass() {
		Map<String, Object> consumerProps =
				KafkaTestUtils.consumerProps(CONSUMER_GROUP_ID, "false", EmbeddedKafkaCondition.getBroker());
		reactiveKafkaConsumerTemplate =
				new ReactiveKafkaConsumerTemplate<>(setupReceiverOptionsWithDefaultTopic(consumerProps));
	}

	@BeforeEach
	public void setUp() {
		reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate<>(setupSenderOptionsWithDefaultTopic(),
				new MessagingMessageConverter());
	}

	private SenderOptions<Integer, String> setupSenderOptionsWithDefaultTopic() {
		Map<String, Object> senderProps =
				KafkaTestUtils.producerProps(EmbeddedKafkaCondition.getBroker().getBrokersAsString());
		SenderOptions<Integer, String> senderOptions = SenderOptions.create(senderProps);
		senderOptions = senderOptions
				.producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "reactive.transaction")
				.producerProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		return senderOptions;
	}

	private static ReceiverOptions<Integer, String> setupReceiverOptionsWithDefaultTopic(
			Map<String, Object> consumerProps) {

		ReceiverOptions<Integer, String> basicReceiverOptions = ReceiverOptions.create(consumerProps);
		return basicReceiverOptions
				.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
				.consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
				.consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
				.addAssignListener(p -> assertThat(p.iterator().next().topicPartition().topic())
						.isEqualTo(REACTIVE_INT_KEY_TOPIC))
				.subscription(Collections.singletonList(REACTIVE_INT_KEY_TOPIC));
	}

	@AfterEach
	public void tearDown() {
		reactiveKafkaProducerTemplate.close();
	}

	@Test
	public void shouldNotCreateTemplateIfOptionsIsNull() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ReactiveKafkaConsumerTemplate<String, String>(null))
				.withMessage("Receiver options can not be null");
	}

	@Test
	public void shouldSendOneRecordTransactionallyViaTemplateAsSenderRecordAndReceiveIt() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY,
						DEFAULT_VALUE);
		long correlationMetadata = 1L;
		SenderRecord<Integer, String, Long> senderRecord = SenderRecord.create(producerRecord, correlationMetadata);

		Mono<SenderResult<Long>> publisher = reactiveKafkaProducerTemplate.sendTransactionally(senderRecord);
		StepVerifier.create(publisher)
				.assertNext(senderResult -> {
					assertThat(senderRecord.correlationMetadata()).isEqualTo(correlationMetadata);
					assertThat(senderResult.recordMetadata())
							.extracting(RecordMetadata::topic, RecordMetadata::partition, RecordMetadata::timestamp)
							.containsExactly(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP);
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
					assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
					assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendOneRecordTransactionallyViaTemplateAsPublisherAndReceiveIt() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY,
						DEFAULT_VALUE);

		Flux<SenderRecord<Integer, String, Long>> senderRecordsGroupTransaction = Flux
				.just(SenderRecord.create(producerRecord, 1L));

		StepVerifier.create(reactiveKafkaProducerTemplate.sendTransactionally(senderRecordsGroupTransaction).then())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
					assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
					assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
					assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@Test
	public void shouldSendMultipleRecordsTransactionallyViaTemplateAndReceiveIt() {
		int recordsCountInGroup = 10;
		int transactionGroupsCount = 1;
		int expectedTotalRecordsCount = recordsCountInGroup * transactionGroupsCount;

		Flux<SenderRecord<Integer, String, Integer>> groupTransactions = generateSenderRecords(recordsCountInGroup, 1);

		StepVerifier.create(reactiveKafkaProducerTemplate.sendTransactionally(groupTransactions).then())
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		Flux<ReceiverRecord<Integer, String>> receiverRecordFlux = reactiveKafkaConsumerTemplate.receive()
				.doOnNext(rr -> rr.receiverOffset().acknowledge()).take(expectedTotalRecordsCount);

		StepVerifier.create(receiverRecordFlux)
				.recordWith(ArrayList::new)
				.expectNextCount(expectedTotalRecordsCount)
				.consumeSubscriptionWith(Subscription::cancel)
				.consumeRecordedWith(receiverRecords -> {
					assertThat(receiverRecords).hasSize(expectedTotalRecordsCount);

					//check first record value
					ReceiverRecord<Integer, String> firstRecord = receiverRecords.iterator().next();
					assertThat(firstRecord.value()).endsWith("10");

					receiverRecords.forEach(receiverRecord -> {
						assertThat(receiverRecord.partition()).isEqualTo(DEFAULT_PARTITION);
						assertThat(receiverRecord.timestamp()).isEqualTo(DEFAULT_TIMESTAMP);
						assertThat(receiverRecord.key()).isEqualTo(DEFAULT_KEY);
						assertThat(receiverRecord.value()).startsWith(DEFAULT_VALUE);
					});

					//check last record value
					Optional<ReceiverRecord<Integer, String>> lastRecord = receiverRecords.stream()
							.skip(expectedTotalRecordsCount - 1).findFirst();
					assertThat(lastRecord.isPresent()).isEqualTo(true);
					lastRecord.ifPresent(last -> assertThat(last.value())
							.endsWith(
									String.valueOf(recordsCountInGroup * (int) Math.pow(10, transactionGroupsCount))));
				})
				.expectComplete()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	private Flux<SenderRecord<Integer, String, Integer>> generateSenderRecords(int recordsCount, int seed) {
		return Flux.range(1, recordsCount)
				.map(i -> {
					int correlationMetadata = i * (int) Math.pow(10, seed);
					return SenderRecord
							.create(new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP,
									DEFAULT_KEY, DEFAULT_VALUE + correlationMetadata), correlationMetadata);
				});
	}

	@Test
	public void shouldSendOneRecordTransactionallyViaTemplateAsSenderRecordAndReceiveItExactlyOnceWithException() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY,
						DEFAULT_VALUE);

		StepVerifier.create(reactiveKafkaProducerTemplate
				.sendTransactionally(SenderRecord.create(producerRecord, null))
				.then())
				.expectComplete()
				.verify();

		StepVerifier.create(reactiveKafkaConsumerTemplate
				.receiveExactlyOnce(reactiveKafkaProducerTemplate.transactionManager())
				.concatMap(consumerRecordFlux -> sendAndCommit(consumerRecordFlux, true))
				.onErrorResume(error -> reactiveKafkaProducerTemplate.transactionManager()
						.abort()
						.then(Mono.error(error))))
				.expectErrorMatches(throwable -> throwable instanceof KafkaException &&
						throwable.getMessage().equals("TransactionalId reactive.transaction: Invalid transition " +
								"attempted from state READY to state ABORTING_TRANSACTION"))
				.verify();

		StepVerifier.create(reactiveKafkaConsumerTemplate
				.receive().doOnNext(receiverRecord -> receiverRecord.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE))
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	@LogLevels(categories = "reactor.kafka.receiver.internals.ConsumerEventLoop", level = "TRACE",
			classes = { JUnitUtils.class, LogLevelsCondition.class,
					DefaultKafkaSender.class,
					ReactiveKafkaProducerTemplateTransactionIntegrationTests.class})
	@Test
	public void shouldSendOneRecordTransactionallyViaTemplateAsSenderRecordAndReceiveItExactlyOnce() {
		ProducerRecord<Integer, String> producerRecord =
				new ProducerRecord<>(REACTIVE_INT_KEY_TOPIC, DEFAULT_PARTITION, DEFAULT_TIMESTAMP, DEFAULT_KEY,
						DEFAULT_VALUE);

		StepVerifier.create(reactiveKafkaProducerTemplate.sendTransactionally(SenderRecord.create(producerRecord, null))
				.then())
				.expectComplete()
				.verify();

		StepVerifier.create(
				reactiveKafkaConsumerTemplate
						.receiveExactlyOnce(this.reactiveKafkaProducerTemplate.transactionManager())
						.concatMap(consumerRecordFlux -> sendAndCommit(consumerRecordFlux, false)))
				.assertNext(senderResult -> {
					assertThat(senderResult.correlationMetadata().intValue()).isEqualTo(DEFAULT_KEY);
					assertThat(senderResult.recordMetadata().offset()).isGreaterThan(0);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);

		StepVerifier.create(
				reactiveKafkaConsumerTemplate
						.receive().doOnNext(receiverRecord -> receiverRecord.receiverOffset().acknowledge()))
				.assertNext(receiverRecord -> {
					logger.info(ListenerUtils.recordToString(receiverRecord, true));
					assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE + "xyz");
					assertThat(receiverRecord.offset()).isGreaterThan(0);
				})
				.thenCancel()
				.verify(DEFAULT_VERIFY_TIMEOUT);
	}

	private Flux<SenderResult<Integer>> sendAndCommit(Flux<ConsumerRecord<Integer, String>> fluxConsumerRecord,
			boolean failCommit) {

		return reactiveKafkaProducerTemplate
				.send(fluxConsumerRecord.map(this::toSenderRecord)
						.concatWith(failCommit ?
								doThrowKafkaException() :
								reactiveKafkaProducerTemplate.transactionManager().commit()));
	}

	private Publisher<? extends SenderRecord<Integer, String, Integer>> doThrowKafkaException() {
		throw new KafkaException();
	}

	private SenderRecord<Integer, String, Integer> toSenderRecord(ConsumerRecord<Integer, String> record) {
		logger.info(ListenerUtils.recordToString(record, true));
		return SenderRecord.create(REACTIVE_INT_KEY_TOPIC, record.partition(), null, record.key(),
				record.value() + "xyz", record.key());
	}

}
