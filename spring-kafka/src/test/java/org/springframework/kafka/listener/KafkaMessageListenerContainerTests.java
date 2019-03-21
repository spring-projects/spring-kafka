/*
 * Copyright 2016-2017 the original author or authors.
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
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
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
 * @author Loic Talhouarne
 * @author Thorsten Binias
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

	private static String topic8 = "testTopic8";

	private static String topic9 = "testTopic9";

	private static String topic10 = "testTopic10";

	private static String topic11 = "testTopic11";

	private static String topic12 = "testTopic12";

	private static String topic13 = "testTopic13";

	private static String topic14 = "testTopic14";

	private static String topic15 = "testTopic15";

	private static String topic16 = "testTopic16";

	private static String topic17 = "testTopic17";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5,
			topic6, topic7, topic8, topic9, topic10, topic11, topic12, topic13, topic14, topic15, topic16, topic17);

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testSlowListener() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow1", "false", embeddedKafka);
//		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 6); // 2 per poll
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
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
		verify(consumer, atLeastOnce()).pause(anyObject());
		verify(consumer, atLeastOnce()).resume(anyObject());
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
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
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

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testSlow2");
		container.start();

		Consumer<?, ?> consumer = spyOnConsumer(container);

		final CountDownLatch commitLatch = new CountDownLatch(7);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				commitLatch.countDown();
			}

		}).given(consumer)
				.commitSync(any());
		stubbingComplete.countDown();

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
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(anyObject());
		verify(consumer, atLeastOnce()).resume(anyObject());
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

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);

		container.setBeanName("testSlow");

		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);

		final CountDownLatch latch = new CountDownLatch(3);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				latch.countDown();
			}

		}).given(consumer)
				.commitSync(any());
		stubbingComplete.countDown();

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
		verify(consumer, atLeastOnce()).pause(anyObject());
		verify(consumer, atLeastOnce()).resume(anyObject());
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
			logger.info("flushed: " + message);
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
		verify(consumer, atLeastOnce()).pause(anyObject());
		verify(consumer, atLeastOnce()).resume(anyObject());
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
		verify(consumer, atLeastOnce()).pause(anyObject());
		verify(consumer, atLeastOnce()).resume(anyObject());
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

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testRecordAcks");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgumentAt(0, Map.class);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						latch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(any());
		stubbingComplete.countDown();
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

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchAcks");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch firstBatchLatch = new CountDownLatch(1);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgumentAt(0, Map.class);
			for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
				if (entry.getValue().offset() == 2) {
					firstBatchLatch.countDown();
				}
			}
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						latch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(any());
		stubbingComplete.countDown();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic7);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

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

	@Test
	public void testBatchListener() throws Exception {
		logger.info("Start batch listener");

		Map<String, Object> props = KafkaTestUtils.consumerProps("test8", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic8);
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) messages -> {
			logger.info("batch listener: " + messages);
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchListener");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch firstBatchLatch = new CountDownLatch(1);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgumentAt(0, Map.class);
			for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
				if (entry.getValue().offset() == 2) {
					firstBatchLatch.countDown();
				}
			}
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						latch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(any());
		stubbingComplete.countDown();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic8);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		assertThat(firstBatchLatch.await(9, TimeUnit.SECONDS)).isTrue();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic8, 0), new TopicPartition(topic8, 1)));
		assertThat(consumer.position(new TopicPartition(topic8, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic8, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch listener");
	}

	@Test
	public void testBatchListenerManual() throws Exception {
		logger.info("Start batch listener manual");

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic9);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		Map<String, Object> props = KafkaTestUtils.consumerProps("test9", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic9);
		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (messages, ack) -> {
			logger.info("batch listener manual: " + messages);
			for (int i = 0; i < messages.size(); i++) {
				latch.countDown();
			}
			ack.acknowledge();
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchListenerManual");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		AtomicBoolean smallOffsetCommitted = new AtomicBoolean(false);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgumentAt(0, Map.class);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 1) {
						smallOffsetCommitted.set(true);
					}
					else if (entry.getValue().offset() == 2) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(any());
		stubbingComplete.countDown();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(smallOffsetCommitted.get()).isFalse();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic9, 0), new TopicPartition(topic9, 1)));
		assertThat(consumer.position(new TopicPartition(topic9, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic9, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch listener manual");
	}

	@Test
	public void testBatchListenerErrors() throws Exception {
		logger.info("Start batch listener errors");

		Map<String, Object> props = KafkaTestUtils.consumerProps("test9", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic10);
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) messages -> {
			logger.info("batch listener errors: " + messages);
			throw new RuntimeException("intentional");
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(true);
		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setGenericErrorHandler((BatchErrorHandler) (t, messages) -> {
			new BatchLoggingErrorHandler().handle(t, messages);
			for (int i = 0; i < messages.count(); i++) {
				latch.countDown();
			}
		});

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchListenerErrors");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgumentAt(0, Map.class);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(any());
		stubbingComplete.countDown();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic10);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic10, 0), new TopicPartition(topic10, 1)));
		assertThat(consumer.position(new TopicPartition(topic10, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic10, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch listener errors");
	}

	@Test
	public void testSeek() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test11", "false", embeddedKafka);
		testSeekGuts(props, topic11, false, false);
	}

	@Test
	public void testSeekAutoCommit() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test12", "true", embeddedKafka);
		testSeekGuts(props, topic12, true, false);
	}

	@Test
	public void testSeekAutoCommitDefault() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test15", "true", embeddedKafka);
		props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG); // test true by default
		testSeekGuts(props, topic15, true, false);
	}

	@Test
	public void testSeekBatch() throws Exception {
		logger.info("Start seek batch seek");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test16", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic16);
		final CountDownLatch registerLatch = new CountDownLatch(1);
		final CountDownLatch assignedLatch = new CountDownLatch(1);
		final CountDownLatch idleLatch = new CountDownLatch(1);
		class Listener implements BatchMessageListener<Integer, String>, ConsumerSeekAware {

			@Override
			public void onMessage(List<ConsumerRecord<Integer, String>> data) {
				// empty
			}

			@Override
			public void registerSeekCallback(ConsumerSeekCallback callback) {
				registerLatch.countDown();
			}

			@Override
			public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
				assignedLatch.countDown();
			}

			@Override
			public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
				idleLatch.countDown();
			}

		}
		Listener messageListener = new Listener();
		containerProps.setMessageListener(messageListener);
		containerProps.setSyncCommits(true);
		containerProps.setAckOnError(false);
		containerProps.setIdleEventInterval(10L);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("testBatchSeek");
		container.start();
		assertThat(registerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(assignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(idleLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	public void testSeekAck() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test17", "false", embeddedKafka);
		testSeekGuts(props, topic17, false, true);
	}

	private void testSeekGuts(Map<String, Object> props, String topic, boolean autoCommit, boolean acknowledging) throws Exception {
		logger.info("Start seek " + topic);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(6));
		final AtomicBoolean seekInitial = new AtomicBoolean();
		final CountDownLatch idleLatch = new CountDownLatch(1);
		class Listener implements MessageListener<Integer, String>, ConsumerSeekAware {

			private ConsumerSeekCallback callback;

			private Thread registerThread;

			private Thread messageThread;

			@Override
			public void onMessage(ConsumerRecord<Integer, String> data) {
				messageThread = Thread.currentThread();
				latch.get().countDown();
				if (latch.get().getCount() == 2 && !seekInitial.get()) {
					callback.seek(topic, 0, 1);
					callback.seek(topic, 1, 1);
				}
			}

			@Override
			public void registerSeekCallback(ConsumerSeekCallback callback) {
				this.callback = callback;
				this.registerThread = Thread.currentThread();
			}

			@Override
			public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
					ConsumerSeekCallback callback) {
				if (seekInitial.get()) {
					for (Entry<TopicPartition, Long> assignment : assignments.entrySet()) {
						callback.seek(assignment.getKey().topic(), assignment.getKey().partition(),
								assignment.getValue() - 1);
					}
				}
			}

			@Override
			public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
				for (Entry<TopicPartition, Long> assignment : assignments.entrySet()) {
					callback.seek(assignment.getKey().topic(), assignment.getKey().partition(),
							assignment.getValue() - 1);
				}
				idleLatch.countDown();
			}

		}

		class AcknowledgingListener extends Listener implements AcknowledgingMessageListener<Integer, String> {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
				super.onMessage(data);
			}

		}

		Listener messageListener = acknowledging ? new AcknowledgingListener() : new Listener();
		containerProps.setMessageListener(messageListener);
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setAckOnError(false);
		containerProps.setIdleEventInterval(60000L);

		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("testSeek" + topic);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.autoCommit", Boolean.class))
				.isEqualTo(autoCommit);
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(latch.get().await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(messageListener.registerThread).isSameAs(messageListener.messageThread);

		// Now test initial seek of assigned partitions.
		latch.set(new CountDownLatch(2));
		seekInitial.set(true);
		container.start();
		assertThat(latch.get().await(60, TimeUnit.SECONDS)).isTrue();

		// Now seek on idle
		latch.set(new CountDownLatch(2));
		seekInitial.set(true);
		container.getContainerProperties().setIdleEventInterval(100L);
		final AtomicBoolean idleEventPublished = new AtomicBoolean();
		container.setApplicationEventPublisher(new ApplicationEventPublisher() {

			@Override
			public void publishEvent(Object event) {
				// NOSONAR
			}

			@Override
			public void publishEvent(ApplicationEvent event) {
				idleEventPublished.set(true);
			}

		});
		assertThat(idleLatch.await(60, TimeUnit.SECONDS));
		assertThat(idleEventPublished.get()).isTrue();
		assertThat(latch.get().await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop seek");
	}

	@Test
	public void testDefinedPartitions() throws Exception {
		this.logger.info("Start defined parts");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test3", "false", embeddedKafka);
		TopicPartitionInitialOffset topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, 0L);

		CountDownLatch initialConsumersLatch = new CountDownLatch(2);

		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			public Consumer<Integer, String> createConsumer(String clientIdPrefix) {
				return new KafkaConsumer<Integer, String>(props) {

					@Override
					public ConsumerRecords<Integer, String> poll(long timeout) {
						try {
							return super.poll(timeout);
						}
						finally {
							initialConsumersLatch.countDown();
						}
					}

				};
			}

		};

		ContainerProperties container1Props = new ContainerProperties(topic1Partition0);
		CountDownLatch latch1 = new CountDownLatch(2);
		container1Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part: " + message);
			latch1.countDown();
		});
		CountDownLatch stubbingComplete1 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container1 = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, container1Props), stubbingComplete1);
		container1.setBeanName("b1");
		container1.start();

		CountDownLatch stopLatch1 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch1.countDown();
			}

		}).given(spyOnConsumer(container1))
				.commitSync(any());
		stubbingComplete1.countDown();

		TopicPartitionInitialOffset topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, 0L);
		ContainerProperties container2Props = new ContainerProperties(topic1Partition1);
		CountDownLatch latch2 = new CountDownLatch(2);
		container2Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part: " + message);
			latch2.countDown();
		});
		CountDownLatch stubbingComplete2 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container2 = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, container2Props), stubbingComplete2);
		container2.setBeanName("b2");
		container2.start();

		CountDownLatch stopLatch2 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch2.countDown();
			}

		}).given(spyOnConsumer(container2))
				.commitSync(any());
		stubbingComplete2.countDown();

		assertThat(initialConsumersLatch.await(20, TimeUnit.SECONDS)).isTrue();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic13);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 2, "qux");
		template.flush();

		assertThat(latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(latch2.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch1.await(60, TimeUnit.SECONDS)).isTrue();
		container1.stop();
		assertThat(stopLatch2.await(60, TimeUnit.SECONDS)).isTrue();
		container2.stop();

		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset earliest
		ContainerProperties container3Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		CountDownLatch latch3 = new CountDownLatch(4);
		container3Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part e: " + message);
			latch3.countDown();
		});

		final CountDownLatch listenerConsumerAvailableLatch = new CountDownLatch(1);

		final CountDownLatch listenerConsumerStartLatch = new CountDownLatch(1);

		CountDownLatch stubbingComplete3 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> resettingContainer = spyOnContainer(
				new KafkaMessageListenerContainer<Integer, String>(cf, container3Props), stubbingComplete3);
		stubSetRunning(listenerConsumerAvailableLatch, listenerConsumerStartLatch, resettingContainer);
		resettingContainer.setBeanName("b3");

		Executors.newSingleThreadExecutor().submit(resettingContainer::start);

		CountDownLatch stopLatch3 = new CountDownLatch(1);

		assertThat(listenerConsumerAvailableLatch.await(60, TimeUnit.SECONDS)).isTrue();

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch3.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(any());
		stubbingComplete3.countDown();

		listenerConsumerStartLatch.countDown();

		assertThat(latch3.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch3.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(latch3.getCount()).isEqualTo(0L);

		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset beginning for part 0, minus one for part 1
		topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, -1000L);
		topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, -1L);
		ContainerProperties container4Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		CountDownLatch latch4 = new CountDownLatch(3);
		AtomicReference<String> receivedMessage = new AtomicReference<>();
		container4Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part 0, -1: " + message);
			receivedMessage.set(message.value());
			latch4.countDown();
		});

		CountDownLatch stubbingComplete4 = new CountDownLatch(1);
		resettingContainer = spyOnContainer(new KafkaMessageListenerContainer<>(cf, container4Props),
				stubbingComplete4);
		resettingContainer.setBeanName("b4");

		resettingContainer.start();

		CountDownLatch stopLatch4 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch4.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(any());
		stubbingComplete4.countDown();

		assertThat(latch4.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch4.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(receivedMessage.get()).isIn("baz", "qux");
		assertThat(latch4.getCount()).isEqualTo(0L);

		// reset plus one
		template.sendDefault(0, 0, "FOO");
		template.sendDefault(1, 2, "BAR");
		template.flush();

		topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, 1L);
		topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, 1L);
		ContainerProperties container5Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		final CountDownLatch latch5 = new CountDownLatch(4);
		final List<String> messages = new ArrayList<>();
		container5Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part 1: " + message);
			messages.add(message.value());
			latch5.countDown();
		});

		CountDownLatch stubbingComplete5 = new CountDownLatch(1);
		resettingContainer = spyOnContainer(new KafkaMessageListenerContainer<>(cf, container5Props),
				stubbingComplete5);
		resettingContainer.setBeanName("b5");
		resettingContainer.start();

		CountDownLatch stopLatch5 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch5.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(any());
		stubbingComplete5.countDown();

		assertThat(latch5.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch5.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(messages).contains("baz", "qux", "FOO", "BAR");

		this.logger.info("+++++++++++++++++++++ Start relative reset");

		template.sendDefault(0, 0, "BAZ");
		template.sendDefault(1, 2, "QUX");
		template.sendDefault(0, 0, "FIZ");
		template.sendDefault(1, 2, "BUZ");
		template.flush();

		topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, 1L, true);
		topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, -1L, true);
		ContainerProperties container6Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		final CountDownLatch latch6 = new CountDownLatch(4);
		final List<String> messages6 = new ArrayList<>();
		container6Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part relative: " + message);
			messages6.add(message.value());
			latch6.countDown();
		});

		CountDownLatch stubbingComplete6 = new CountDownLatch(1);
		resettingContainer = spyOnContainer(new KafkaMessageListenerContainer<>(cf, container6Props),
				stubbingComplete6);
		resettingContainer.setBeanName("b6");
		resettingContainer.start();

		CountDownLatch stopLatch6 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch6.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(any());
		stubbingComplete6.countDown();

		assertThat(latch6.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch6.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(messages6).hasSize(4);
		assertThat(messages6).contains("FIZ", "BAR", "QUX", "BUZ");

		this.logger.info("Stop auto parts");
	}

	private void stubSetRunning(final CountDownLatch listenerConsumerAvailableLatch,
			final CountDownLatch listenerConsumerStartLatch,
			KafkaMessageListenerContainer<Integer, String> resettingContainer) {
		willAnswer(invocation -> {
			listenerConsumerAvailableLatch.countDown();
			try {
				assertThat(listenerConsumerStartLatch.await(10, TimeUnit.SECONDS)).isTrue();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
			return invocation.callRealMethod();
		}).given(resettingContainer).setRunning(true);
	}

	@Test
	public void testManualAckRebalance() throws Exception {
		logger.info("Start manual ack rebalance");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test14", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic14);
		final List<AtomicInteger> counts = new ArrayList<>();
		counts.add(new AtomicInteger());
		counts.add(new AtomicInteger());
		final Acknowledgment[] pendingAcks = new Acknowledgment[2];
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			logger.info("manual ack: " + message);
			if (counts.get(message.partition()).incrementAndGet() < 2) {
				ack.acknowledge();
			}
			else {
				pendingAcks[message.partition()] = ack;
			}
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		final CountDownLatch rebalanceLatch = new CountDownLatch(2);
		containerProps.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				logger.info("manual ack: revoked " + partitions);
				partitions.forEach(p -> {
					if (pendingAcks[p.partition()] != null) {
						pendingAcks[p.partition()].acknowledge();
						pendingAcks[p.partition()] = null;
					}
				});
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("manual ack: assigned " + partitions);
				rebalanceLatch.countDown();
			}
		});

		CountDownLatch stubbingComplete1 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container1 = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete1);
		container1.setBeanName("testAckRebalance");
		container1.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container1);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(invocation -> {

			@SuppressWarnings({ "unchecked" })
			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgumentAt(0, Map.class);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 1) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(any());
		stubbingComplete1.countDown();
		ContainerTestUtils.waitForAssignment(container1, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic14);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "baz");
		template.sendDefault(0, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer<Integer, String> container2 = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container2.setBeanName("testAckRebalance2");
		container2.start();
		assertThat(rebalanceLatch.await(60, TimeUnit.SECONDS)).isTrue();
		container1.stop();
		container2.stop();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic14, 0), new TopicPartition(topic14, 1)));
		assertThat(consumer.position(new TopicPartition(topic14, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic14, 1))).isEqualTo(2);
		consumer.close();
		logger.info("Stop manual ack rebalance");
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

	private KafkaMessageListenerContainer<Integer, String> spyOnContainer(KafkaMessageListenerContainer<Integer, String> container,
			final CountDownLatch stubbingComplete) {
		KafkaMessageListenerContainer<Integer, String> spy = spy(container);
		willAnswer(i -> {
			if (stubbingComplete.getCount() > 0 && Thread.currentThread().getName().endsWith("-C-1")) {
				try {
					stubbingComplete.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			return i.callRealMethod();
		}).given(spy).isRunning();
		return spy;
	}

	@SuppressWarnings("serial")
	public static class FooEx extends RuntimeException {

	}

}
