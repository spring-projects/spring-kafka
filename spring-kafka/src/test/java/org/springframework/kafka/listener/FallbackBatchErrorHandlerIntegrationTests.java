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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.3.7
 *
 */
@EmbeddedKafka(topics = {
		FallbackBatchErrorHandlerIntegrationTests.topic1,
		FallbackBatchErrorHandlerIntegrationTests.topic1DLT,
		FallbackBatchErrorHandlerIntegrationTests.topic2,
		FallbackBatchErrorHandlerIntegrationTests.topic2DLT})
public class FallbackBatchErrorHandlerIntegrationTests {

	public static final String topic1 = "retryTopic1";

	public static final String topic1DLT = "retryTopic1.DLT";

	public static final String topic2 = "retryTopic2";

	public static final String topic2DLT = "retryTopic2.DLT";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testRetriesAndDlt() throws InterruptedException {
		Map<String, Object> props = KafkaTestUtils.consumerProps("retryBatch", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		final KafkaOperations<Object, Object> template = new KafkaTemplate<>(pf);
		final CountDownLatch latch = new CountDownLatch(3);
		AtomicReference<List<ConsumerRecord<Integer, String>>> data = new AtomicReference<>();
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) records -> {
			data.set(records);
			latch.countDown();
			throw new ListenerExecutionFailedException("fail for retry batch");
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("retryBatch");
		final CountDownLatch recoverLatch = new CountDownLatch(1);
		final AtomicReference<String> failedGroupId = new AtomicReference<>();
		DeadLetterPublishingRecoverer recoverer =
				new DeadLetterPublishingRecoverer(template,
						(r, e) -> new TopicPartition(topic1DLT, r.partition())) {

			@Override
			public void accept(ConsumerRecord<?, ?> record, Exception exception) {
				super.accept(record, exception);
				if (exception instanceof ListenerExecutionFailedException) {
					failedGroupId.set(((ListenerExecutionFailedException) exception).getGroupId());
				}
				recoverLatch.countDown();
			}

		};
		FallbackBatchErrorHandler errorHandler = new FallbackBatchErrorHandler(new FixedBackOff(0L, 3), recoverer);
		container.setBatchErrorHandler(errorHandler);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		template.send(topic1, 0, 0, "foo");
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data.get()).hasSize(1);
		assertThat(data.get().iterator().next().value()).isEqualTo("foo");
		assertThat(recoverLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(failedGroupId.get()).isEqualTo("retryBatch");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "retryBatch.dlt");
		DefaultKafkaConsumerFactory<Integer, String> dltcf = new DefaultKafkaConsumerFactory<>(props);
		Consumer<Integer, String> consumer = dltcf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic1DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic1DLT);
		assertThat(dltRecord.value()).isEqualTo("foo");
		container.stop();
		pf.destroy();
		consumer.close();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testRetriesCantRecover() throws InterruptedException {
		Map<String, Object> props = KafkaTestUtils.consumerProps("retryBatch2", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic2);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		final KafkaOperations<Object, Object> template = new KafkaTemplate<>(pf);
		final CountDownLatch latch = new CountDownLatch(6);
		AtomicReference<List<ConsumerRecord<Integer, String>>> data = new AtomicReference<>();
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) records -> {
			data.set(records);
			latch.countDown();
			throw new ListenerExecutionFailedException("fail for retry batch");
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("retryBatch");
		final CountDownLatch recoverLatch = new CountDownLatch(1);
		final AtomicReference<String> failedGroupId = new AtomicReference<>();
		final AtomicBoolean failRecovery = new AtomicBoolean(true);
		DeadLetterPublishingRecoverer recoverer =
				new DeadLetterPublishingRecoverer(template,
						(r, e) -> new TopicPartition(topic2DLT, r.partition())) {

			@Override
			public void accept(ConsumerRecord<?, ?> record, Exception exception) {
				if (exception instanceof ListenerExecutionFailedException) {
					failedGroupId.set(((ListenerExecutionFailedException) exception).getGroupId());
				}
				if (failRecovery.getAndSet(false)) {
					throw new RuntimeException("Recovery failed");
				}
				super.accept(record, exception);
				recoverLatch.countDown();
			}

		};
		FallbackBatchErrorHandler errorHandler = new FallbackBatchErrorHandler(new FixedBackOff(0L, 3), recoverer);
		container.setBatchErrorHandler(errorHandler);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		template.send(topic2, 0, 0, "foo");
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data.get()).hasSize(1);
		assertThat(data.get().iterator().next().value()).isEqualTo("foo");
		assertThat(recoverLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(failedGroupId.get()).isEqualTo("retryBatch2");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "retryBatch2.dlt");
		DefaultKafkaConsumerFactory<Integer, String> dltcf = new DefaultKafkaConsumerFactory<>(props);
		Consumer<Integer, String> consumer = dltcf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic2DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic2DLT);
		assertThat(dltRecord.value()).isEqualTo("foo");
		container.stop();
		pf.destroy();
		consumer.close();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	@Test
	void consumerEx() throws InterruptedException {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(consumer.poll(any())).willThrow(new RuntimeException("test"));
		given(cf.createConsumer(any(), any(), isNull(), any())).willReturn(consumer);
		ContainerProperties containerProps = new ContainerProperties(new TopicPartitionOffset("foo", 0));
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		CountDownLatch called = new CountDownLatch(1);
		container.setBatchErrorHandler(new FallbackBatchErrorHandler() {

			@Override
			public void handle(Exception thrownException, ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
					MessageListenerContainer container, Runnable invokeListener) {

				called.countDown();
				super.handle(thrownException, records, consumer, container, invokeListener);
			}
		});
		container.setupMessageListener((BatchMessageListener<Integer, String>) (recs -> { }));
		container.start();
		assertThat(called.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

}
