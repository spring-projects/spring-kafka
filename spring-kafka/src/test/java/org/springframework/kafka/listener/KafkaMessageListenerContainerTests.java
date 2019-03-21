/*
 * Copyright 2016 the original author or authors.
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
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * Tests for the listener container.
 *
 * @author Gary Russell
 * @author Martin Dam
 * @author Artem Bilan
 */
public class KafkaMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	private static String topic6 = "testTopic6";

	private static String topic7 = "testTopic7";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5,
			topic6, topic7);

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testSlowListener() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow1", "false", embeddedKafka);
//		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 6); // 2 per poll
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);

		final CountDownLatch latch = new CountDownLatch(6);
		final BitSet bitSet = new BitSet(6);
		final AtomicReference<Acknowledgment> ackNull = new AtomicReference<>();
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			logger.info("slow1: " + message);
			bitSet.set((int) (message.partition() * 3 + message.offset()));
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			ackNull.set(ack);
			latch.countDown();
		});
		containerProps.setPauseAfter(100);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testSlow1");


		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(ackNull.get()).isNull();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName());
	}

	@Test
	public void testSlowListenerManualCommit() throws Exception {
		testSlowListenerManualGuts(AckMode.MANUAL_IMMEDIATE, topic2);
		// to be sure the commits worked ok so run the tests again and the second tests start at the committed offset.
		testSlowListenerManualGuts(AckMode.MANUAL_IMMEDIATE, topic2);
	}

	private void testSlowListenerManualGuts(AckMode ackMode, String topic) throws Exception {
		logger.info("Start " + this.testName.getMethodName() + ackMode);
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow2", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setSyncCommits(true);

		final CountDownLatch latch = new CountDownLatch(6);
		final BitSet bitSet = new BitSet(4);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			logger.info("slow2: " + message);
			bitSet.set((int) (message.partition() * 3 + message.offset()));
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			ack.acknowledge();
			latch.countDown();
		});
		containerProps.setPauseAfter(100);
		containerProps.setAckMode(ackMode);

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testSlow2");
		container.start();

		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName() + ackMode);
	}

	@Test
	public void testSlowConsumerCommitsAreProcessed() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic5);
		containerProps.setAckCount(1);
		containerProps.setPauseAfter(100);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setSyncCommits(true);

		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			logger.info("slow: " + message);
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			ack.acknowledge();
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);

		container.setBeanName("testSlow");

		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);

		final CountDownLatch latch = new CountDownLatch(3);

		willAnswer(invocation -> {

			latch.countDown();
			return invocation.callRealMethod();

		}).given(consumer)
				.commitSync(any());

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic5);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, 0, "fiz");
		template.sendDefault(1, 2, "buz");
		template.flush();

		// Verify that commitSync is called when paused
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
	}

	@Test
	public void testCommitsAreFlushedOnStop() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("flushedOnStop", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic5);
		containerProps.setAckCount(1);
		containerProps.setPauseAfter(100);
		// set large values, ensuring that commits don't happen before `stop()`
		containerProps.setAckTime(20000);
		containerProps.setAckCount(20000);
		containerProps.setAckMode(AckMode.COUNT_TIME);

		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("slow: " + message);
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testManualFlushed");

		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic5);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, 0, "fiz");
		template.sendDefault(1, 2, "buz");
		template.flush();

		// Verify that commitSync is called when paused
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		// Verify that just the initial commit is processed before stop
		verify(consumer, times(1)).commitSync(any());
		container.stop();
		// Verify that a commit has been made on stop
		verify(consumer, times(2)).commitSync(any());
	}

	@Test
	public void testSlowConsumerWithException() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow3", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic3);

		final CountDownLatch latch = new CountDownLatch(18);
		final BitSet bitSet = new BitSet(6);
		final Map<String, AtomicInteger> faults = new HashMap<>();
		RetryingMessageListenerAdapter<Integer, String> adapter = new RetryingMessageListenerAdapter<>(
					new MessageListener<Integer, String>() {

				@Override
				public void onMessage(ConsumerRecord<Integer, String> message) {
					logger.info("slow3: " + message);
					bitSet.set((int) (message.partition() * 3 + message.offset()));
					String key = message.topic() + message.partition() + message.offset();
					if (faults.get(key) == null) {
						faults.put(key, new AtomicInteger(1));
					}
					else {
						faults.get(key).incrementAndGet();
					}
					latch.countDown(); // 3 per = 18
					if (faults.get(key).get() < 3) { // succeed on the third attempt
						throw new FooEx();
					}
				}

			}, buildRetry(), null);
		containerProps.setMessageListener(adapter);
		containerProps.setPauseAfter(100);

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testSlow3");

		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName());
	}

	@Test
	public void testSlowConsumerWithSlowThenExceptionThenGood() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow4", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic4);
		final CountDownLatch latch = new CountDownLatch(18);
		final BitSet bitSet = new BitSet(6);
		final Map<String, AtomicInteger> faults = new HashMap<>();
		RetryingMessageListenerAdapter<Integer, String> adapter = new RetryingMessageListenerAdapter<>(
			new MessageListener<Integer, String>() {

				@Override
				public void onMessage(ConsumerRecord<Integer, String> message) {
					logger.info("slow4: " + message);
					bitSet.set((int) (message.partition() * 4 + message.offset()));
					String key = message.topic() + message.partition() + message.offset();
					if (faults.get(key) == null) {
						faults.put(key, new AtomicInteger(1));
					}
					else {
						faults.get(key).incrementAndGet();
					}
					latch.countDown(); // 3 per = 18
					if (faults.get(key).get() == 1) {
						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
					if (faults.get(key).get() < 3) { // succeed on the third attempt
						throw new FooEx();
					}
				}

			}, buildRetry(), null);
		containerProps.setMessageListener(adapter);
		containerProps.setPauseAfter(100);

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testSlow4");

		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic4);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName());
	}

	@Test
	public void testRecordAck() throws Exception {
		logger.info("Start record ack");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test6", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic6);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("record ack: " + message);
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setAckOnError(false);

		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("testRecordAcks");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = (Map<TopicPartition, OffsetAndMetadata>) invocation
					.getArguments()[0];
			for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
				if (entry.getValue().offset() == 2) {
					latch.countDown();
				}
			}
			return invocation.callRealMethod();

		}).given(containerConsumer)
				.commitSync(any());
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic6);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic6, 0), new TopicPartition(topic6, 1)));
		assertThat(consumer.position(new TopicPartition(topic6, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic6, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop record ack");
	}

	@Test
	public void testBatchAck() throws Exception {
		logger.info("Start batch ack");

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic7);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		Map<String, Object> props = KafkaTestUtils.consumerProps("test6", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic7);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("batch ack: " + message);
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("testBatchAcks");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch firstBatchLatch = new CountDownLatch(1);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = (Map<TopicPartition, OffsetAndMetadata>) invocation
					.getArguments()[0];
			for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
				if (entry.getValue().offset() == 2) {
					firstBatchLatch.countDown();
					latch.countDown();
				}
			}
			return invocation.callRealMethod();

		}).given(containerConsumer)
				.commitSync(any());

		assertThat(firstBatchLatch.await(9, TimeUnit.SECONDS)).isTrue();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic7, 0), new TopicPartition(topic7, 1)));
		assertThat(consumer.position(new TopicPartition(topic7, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic7, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch ack");
	}

	private RetryTemplate buildRetry() {
		SimpleRetryPolicy policy = new SimpleRetryPolicy(3, Collections.singletonMap(FooEx.class, true));
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(policy);
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		retryTemplate.setBackOffPolicy(backOffPolicy);
		return retryTemplate;
	}

	private Consumer<?, ?> spyOnConsumer(KafkaMessageListenerContainer<Integer, String> container) {
		Consumer<?, ?> consumer = spy(
				KafkaTestUtils.getPropertyValue(container, "listenerConsumer.consumer", Consumer.class));
		new DirectFieldAccessor(KafkaTestUtils.getPropertyValue(container, "listenerConsumer"))
				.setPropertyValue("consumer", consumer);
		return consumer;
	}

	@SuppressWarnings("serial")
	public static class FooEx extends RuntimeException {

	}

}
