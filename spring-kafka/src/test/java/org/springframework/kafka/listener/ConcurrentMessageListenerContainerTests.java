/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public class ConcurrentMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	private static String topic6 = "testTopic6";

	private static String topic7 = "testTopic7";

	private static String topic8 = "testTopic8";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5,
			topic6, topic7, topic8);

	@Test
	public void testAutoCommit() throws Exception {
		logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic1);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto: " + message);
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop auto");
	}

	@Test
	public void testAfterListenCommit() throws Exception {
		logger.info("Start manual");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test2", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic2);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("manual: " + message);
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testBatch");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic2);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop manual");
	}

	@Test
	public void testDefinedPartitions() throws Exception {
		logger.info("Start auto parts");
		final Map<String, Object> props = KafkaTestUtils.consumerProps("test3", "true", embeddedKafka);
		TopicPartition topic1Partition0 = new TopicPartition(topic3, 0);

		final CountDownLatch initialConsumersLatch = new CountDownLatch(2);

		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			public Consumer<Integer, String> createConsumer() {
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

		ConcurrentMessageListenerContainer<Integer, String> container1 =
				new ConcurrentMessageListenerContainer<>(cf, topic1Partition0);
		final CountDownLatch latch1 = new CountDownLatch(2);
		container1.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part: " + message);
				latch1.countDown();
			}

		});
		container1.setBeanName("b1");
		container1.start();

		TopicPartition topic1Partition1 = new TopicPartition(topic3, 1);
		ConcurrentMessageListenerContainer<Integer, String> container2 =
				new ConcurrentMessageListenerContainer<>(cf, topic1Partition1);
		final CountDownLatch latch2 = new CountDownLatch(2);
		container2.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part: " + message);
				latch2.countDown();
			}

		});
		container2.setBeanName("b2");
		container2.start();

		assertThat(initialConsumersLatch.await(20, TimeUnit.SECONDS)).isTrue();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();

		assertThat(latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(latch2.await(60, TimeUnit.SECONDS)).isTrue();
		container1.stop();
		container2.stop();

		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset earliest
		ConcurrentMessageListenerContainer<Integer, String> resettingContainer =
				new ConcurrentMessageListenerContainer<>(cf, topic1Partition0, topic1Partition1);
		resettingContainer.setBeanName("b3");
		final CountDownLatch latch3 = new CountDownLatch(4);
		resettingContainer.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part e: " + message);
				latch3.countDown();
			}
		});
		resettingContainer.start();
		assertThat(latch3.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(latch3.getCount()).isEqualTo(0L);

		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset minusone
		resettingContainer = new ConcurrentMessageListenerContainer<>(cf, topic1Partition0, topic1Partition1);
		resettingContainer.setBeanName("b4");
		resettingContainer.setRecentOffset(1);
		final CountDownLatch latch4 = new CountDownLatch(2);
		final AtomicReference<String> receivedMessage = new AtomicReference<>();
		resettingContainer.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part -1: " + message);
				receivedMessage.set(message.value());
				latch4.countDown();
			}
		});
		resettingContainer.start();
		assertThat(latch4.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(receivedMessage.get()).isIn("baz", "qux");
		assertThat(latch4.getCount()).isEqualTo(0L);

		logger.info("Stop auto parts");
	}

	@Test
	public void testManualCommit() throws Exception {
		testManualCommitGuts(AckMode.MANUAL, topic4);
		testManualCommitGuts(AckMode.MANUAL_IMMEDIATE, topic5);
		// to be sure the commits worked ok so run the tests again and the second tests start at the committed offset.
		testManualCommitGuts(AckMode.MANUAL, topic4);
		testManualCommitGuts(AckMode.MANUAL_IMMEDIATE, topic5);
	}

	private void testManualCommitGuts(AckMode ackMode, String topic) throws Exception {
		logger.info("Start " + ackMode);
		Map<String, Object> props = KafkaTestUtils.consumerProps("test" + ackMode, "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				logger.info("manual: " + message);
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setConcurrency(2);
		container.setAckMode(ackMode);
		container.setBeanName("test" + ackMode);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop " + ackMode);
	}

	@Test
	public void testManualCommitExisting() throws Exception {
		logger.info("Start MANUAL_IMMEDIATE with Existing");
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic7);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();
		Map<String, Object> props = KafkaTestUtils.consumerProps("testManualExisting", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic7);
		final CountDownLatch latch = new CountDownLatch(8);
		container.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				logger.info("manualExisting: " + message);
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setConcurrency(1);
		container.setAckMode(AckMode.MANUAL_IMMEDIATE);
		container.setBeanName("testManualExisting");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		template.send(0, "fooo");
		template.send(2, "barr");
		template.send(0, "bazz");
		template.send(2, "quxx");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop MANUAL_IMMEDIATE with Existing");
	}

	@Test
	public void testManualCommitSyncExisting() throws Exception {
		logger.info("Start MANUAL_IMMEDIATE_SYNC with Existing");
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic8);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();
		Map<String, Object> props = KafkaTestUtils.consumerProps("testManualExistingSync", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic8);
		final CountDownLatch latch = new CountDownLatch(8);
		container.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				logger.info("manualExisting: " + message);
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setConcurrency(1);
		container.setAckMode(AckMode.MANUAL_IMMEDIATE_SYNC);
		container.setBeanName("testManualExisting");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		template.send(0, "fooo");
		template.send(2, "barr");
		template.send(0, "bazz");
		template.send(2, "quxx");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop MANUAL_IMMEDIATE_SYNC with Existing");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testConcurrencyWithPartitions() {
		TopicPartition[] topic1PartitionS = new TopicPartition[]{
				new TopicPartition(topic1, 0),
				new TopicPartition(topic1, 1),
				new TopicPartition(topic1, 2),
				new TopicPartition(topic1, 3),
				new TopicPartition(topic1, 4),
				new TopicPartition(topic1, 5),
				new TopicPartition(topic1, 6)
		};
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer()).willReturn(consumer);
		given(consumer.poll(anyLong()))
			.willAnswer(new Answer<ConsumerRecords<Integer, String>>() {

				@Override
				public ConsumerRecords<Integer, String> answer(InvocationOnMock invocation) throws Throwable {
					Thread.sleep(100);
					return null;
				}

			});
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic1PartitionS);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
			}

		});
		container.setConcurrency(3);
		container.start();
		List<KafkaMessageListenerContainer<Integer, String>> containers =
				(List<KafkaMessageListenerContainer<Integer, String>>) new DirectFieldAccessor(
				container).getPropertyValue("containers");
		assertThat(containers.size()).isEqualTo(3);
		for (int i = 0; i < 3; i++) {
			assertThat(((TopicPartition[]) new DirectFieldAccessor(containers.get(i))
					.getPropertyValue("partitions")).length).isEqualTo(i < 2 ? 2 : 3);
		}
		container.stop();
	}

	@Test
	public void testListenerException() throws Exception {
		logger.info("Start exception");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic6);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto: " + message);
				latch.countDown();
				throw new RuntimeException("intended");
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testException");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic6);
		template.send(0, "foo");
		template.send(2, "bar");
		template.send(0, "baz");
		template.send(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop exception");

	}

}
