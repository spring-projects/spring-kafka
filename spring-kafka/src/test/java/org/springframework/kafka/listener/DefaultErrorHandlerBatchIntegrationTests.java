/*
 * Copyright 2020-2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaOperations2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

/**
 * {@link DefaultErrorHandler} tests for batch listeners. Copied from
 * {@link RecoveringBatchErrorHandlerIntegrationTests} changing error handler type.
 * @author Gary Russell
 * @since 2.8
 *
 */
@EmbeddedKafka(topics = {
		DefaultErrorHandlerBatchIntegrationTests.topic1,
		DefaultErrorHandlerBatchIntegrationTests.topic1DLT,
		DefaultErrorHandlerBatchIntegrationTests.topic2,
		DefaultErrorHandlerBatchIntegrationTests.topic2DLT })
public class DefaultErrorHandlerBatchIntegrationTests {

	public static final String topic1 = "dehTopic1";

	public static final String topic1DLT = "dehTopic1.DLT";

	public static final String topic2 = "dehTopic2";

	public static final String topic2DLT = "dehTopic2.DLT";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	public void recoveryAndDlt() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("recoverBatch", "false", embeddedKafka);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		final KafkaOperations<Object, Object> template = new KafkaTemplate<>(pf);
		final CountDownLatch latch = new CountDownLatch(3);
		List<ConsumerRecord<Integer, String>> data = new ArrayList<>();
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) records -> {
			data.addAll(records);
			latch.countDown();
			records.forEach(rec -> {
				if (rec.value().equals("baz")) {
					throw new BatchListenerFailedException("fail", rec);
				}
			});
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("recoverBatch");
		DeadLetterPublishingRecoverer recoverer =
				new DeadLetterPublishingRecoverer(template,
						(r, e) -> new TopicPartition(topic1DLT, r.partition()));
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 1));
		container.setCommonErrorHandler(errorHandler);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		KafkaOperations2<Object, Object> complete = template.usingCompletableFuture();
		complete.send(topic1, 0, 0, "foo");
		complete.send(topic1, 0, 0, "bar");
		complete.send(topic1, 0, 0, "baz");
		complete.send(topic1, 0, 0, "qux");
		complete.send(topic1, 0, 0, "fiz");
		AtomicReference<SendResult<Object, Object>> sendResult = new AtomicReference<>();
		CompletableFuture<SendResult<Object, Object>> future = complete.send(topic1, 0, 0, "buz")
				.whenComplete((sr, thrown) -> sendResult.set(sr));
		future.get(10, TimeUnit.SECONDS);
		assertThat(sendResult.get()).isNotNull();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data).hasSize(13);
		assertThat(data)
				.extracting(rec -> rec.value())
				.containsExactly(
					"foo", "bar", "baz", "qux", "fiz", "buz",
					"baz", "qux", "fiz", "buz",
					"qux", "fiz", "buz");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "recoverBatch.dlt");
		DefaultKafkaConsumerFactory<Integer, String> dltcf = new DefaultKafkaConsumerFactory<>(props);
		Consumer<Integer, String> consumer = dltcf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic1DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic1DLT);
		assertThat(dltRecord.value()).isEqualTo("baz");
		container.stop();
		pf.destroy();
		consumer.close();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void recoveryFails() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("recoverBatch2", "false", embeddedKafka);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic2);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		final KafkaOperations<Object, Object> template = new KafkaTemplate<>(pf);
		final CountDownLatch latch = new CountDownLatch(4);
		List<ConsumerRecord<Integer, String>> data = new ArrayList<>();
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) records -> {
			data.addAll(records);
			latch.countDown();
			records.forEach(rec -> {
				if (rec.value().equals("baz")) {
					throw new BatchListenerFailedException("fail", rec);
				}
			});
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("recoverBatch");
		final AtomicBoolean failRecovery = new AtomicBoolean(true);
		DeadLetterPublishingRecoverer recoverer =
				new DeadLetterPublishingRecoverer(template,
						(r, e) -> new TopicPartition(topic2DLT, r.partition())) {

			@Override
			public void accept(ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, Exception exception) {
				if (failRecovery.getAndSet(false)) {
					throw new RuntimeException("Recovery failed");
				}
				super.accept(record, consumer, exception);
			}

		};
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 1));
		errorHandler.setResetStateOnRecoveryFailure(false);
		container.setCommonErrorHandler(errorHandler);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		KafkaOperations2<Object, Object> complete = template.usingCompletableFuture();
		complete.send(topic2, 0, 0, "foo");
		complete.send(topic2, 0, 0, "bar");
		complete.send(topic2, 0, 0, "baz");
		complete.send(topic2, 0, 0, "qux");
		complete.send(topic2, 0, 0, "fiz");
		CompletableFuture<SendResult<Object, Object>> future = complete.send(topic2, 0, 0, "buz");
		future.get(10, TimeUnit.SECONDS);
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data).hasSize(17);
		assertThat(data)
				.extracting(rec -> rec.value())
				.containsExactly(
					"foo", "bar", "baz", "qux", "fiz", "buz",
					"baz", "qux", "fiz", "buz",
					// recovery failed first time so we get the whole batch again
					"baz", "qux", "fiz", "buz",
					"qux", "fiz", "buz");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "recoverBatch2.dlt");
		DefaultKafkaConsumerFactory<Integer, String> dltcf = new DefaultKafkaConsumerFactory<>(props);
		Consumer<Integer, String> consumer = dltcf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic2DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic2DLT);
		assertThat(dltRecord.value()).isEqualTo("baz");
		container.stop();
		pf.destroy();
		consumer.close();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

}
